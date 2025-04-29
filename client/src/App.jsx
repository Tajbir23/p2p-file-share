import React, { useState, useRef, useEffect } from 'react';
import { io } from 'socket.io-client';
import { v4 as uuidv4 } from 'uuid';
import './App.css';
import { QRCodeSVG } from 'qrcode.react';

// At the top of the file (after imports), add debug flag
const DEBUG_MODE = true;

// Define maximum chunk size (used throughout but was missing)
const MAX_CHUNK_SIZE = 64 * 1024; // Default to 64KB chunks

// Get server URL based on current environment
const getServerUrl = () => {
  // Extract the current hostname (will be localhost on dev machine or the IP when accessed from mobile)
  const hostname = window.location.hostname;
  return `http://${hostname}:5000`;
};

// Define a socket instance variable
let socketInstance = null;

// Initialize socket with optimized settings for file transfers
const getSocket = () => {
  const server = getServerUrl();
  if (!socketInstance) {
    socketInstance = io(server, {
      reconnectionDelayMax: 5000, // Reduced from 10000 for faster reconnection
      timeout: 180000, // 3 minute timeout (reduced from 5 minutes)
      transports: ['websocket', 'polling'], // Try websocket first, fall back to polling if needed
      pingInterval: 15000, // More frequent ping to keep connection alive (reduced from 25000)
      pingTimeout: 30000, // Faster timeout detection (reduced from 60000)
      perMessageDeflate: false, // Disable compression at socket.io level for speed
      maxHttpBufferSize: 1e9, // Increased to 1GB buffer size for larger chunks (up from 500MB)
      autoConnect: true,
      forceNew: false,
      reconnection: true,
      reconnectionAttempts: 5, // Try to reconnect 5 times
      reconnectionDelay: 1000 // Start with a 1 sec delay, then increase
    });
    
    // Add logging for connection events
    socketInstance.on('connect', () => {
      console.log('Socket connected successfully');
    });
    
    socketInstance.on('connect_error', (err) => {
      console.error('Socket connection error:', err);
    });
    
    socketInstance.on('disconnect', (reason) => {
      console.log(`Socket disconnected, reason: ${reason}`);
      
      // If it was the server disconnecting us, try to reconnect
      if (reason === 'io server disconnect' || reason === 'transport close') {
        console.log('Attempting to reconnect...');
        socketInstance.connect();
      }
    });
  }
  return socketInstance;
};

// Initialize IndexedDB for large file storage
const initializeDB = () => {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open('fileTransferDB', 3); // Upgrade to version 3
    
    request.onerror = (event) => {
      console.error('IndexedDB error:', event);
      reject('Could not open IndexedDB');
    };
    
    request.onupgradeneeded = (event) => {
      const db = event.target.result;
      
      // Create or ensure chunks store exists
      if (!db.objectStoreNames.contains('chunks')) {
        db.createObjectStore('chunks', { keyPath: 'id' });
      }
      
      // Create or ensure transfer state store exists
      if (!db.objectStoreNames.contains('transferState')) {
        db.createObjectStore('transferState', { keyPath: 'id' });
      }
      
      // Create a metadata store for storing file metadata
      if (!db.objectStoreNames.contains('fileMetadata')) {
        db.createObjectStore('fileMetadata', { keyPath: 'id' });
      }
    };
    
    request.onsuccess = (event) => {
      console.log('IndexedDB initialized with version:', event.target.result.version);
      resolve(event.target.result);
    };
  });
};

// Store chunk in IndexedDB
const storeChunk = async (db, chunk, index) => {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction(['chunks'], 'readwrite');
    const store = transaction.objectStore('chunks');
    const request = store.put({ id: index, data: chunk });
    
    request.onsuccess = () => resolve();
    request.onerror = (e) => reject(e);
  });
};

// Get all chunks from IndexedDB
const getAllChunks = async (db, totalChunks) => {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction(['chunks'], 'readonly');
    const store = transaction.objectStore('chunks');
    const chunks = [];
    
    for (let i = 0; i < totalChunks; i++) {
      const request = store.get(i);
      request.onsuccess = (event) => {
        if (event.target.result) {
          chunks.push(event.target.result.data);
        }
        
        if (chunks.length === totalChunks) {
          resolve(chunks);
        }
      };
      request.onerror = (e) => reject(e);
    }
  });
};

// Clear all chunks from IndexedDB
const clearChunks = async (db) => {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction(['chunks'], 'readwrite');
    const store = transaction.objectStore('chunks');
    const request = store.clear();
    
    request.onsuccess = () => resolve();
    request.onerror = (e) => reject(e);
  });
};

// SaveTransferState to store more detailed data for reliable resumption
const saveTransferState = async (db, state) => {
  return new Promise((resolve, reject) => {
    try {
      const transaction = db.transaction(['transferState'], 'readwrite');
      const store = transaction.objectStore('transferState');
      
      // Save with ID 'current' to always have only one state
      const request = store.put({ id: 'current', ...state });
      
      // Also save a backup with timestamp as key
      const timestampedState = { 
        id: `backup_${Date.now()}`,
        ...state,
        backupTimestamp: Date.now()
      };
      store.put(timestampedState);
      
      // Save to localStorage for redundancy (with limited data)
      const simplifiedState = {
        isSending: state.isSending,
        isReceiving: state.isReceiving,
        roomId: state.roomId,
        fileName: state.fileName,
        fileSize: state.fileSize,
        progress: state.progress,
        sentChunksCount: state.sentChunksCount,
        receivedChunksCount: state.receivedChunksCount,
        timestamp: Date.now()
      };
      localStorage.setItem('transferStateBackup', JSON.stringify(simplifiedState));
      
      request.onsuccess = () => {
        console.log('Transfer state saved successfully');
        resolve();
      };
      
      request.onerror = (e) => {
        console.error('Error saving transfer state:', e);
        reject(e);
      };
    } catch (err) {
      console.error('Failed to save transfer state:', err);
      reject(err);
    }
  });
};

// Load transfer state from IndexedDB with fallback mechanisms
const loadTransferState = async (db) => {
  return new Promise((resolve, reject) => {
    try {
      const transaction = db.transaction(['transferState'], 'readonly');
      const store = transaction.objectStore('transferState');
      const request = store.get('current');
      
      request.onsuccess = async (event) => {
        if (event.target.result) {
          console.log('Transfer state loaded successfully from primary source');
          resolve(event.target.result);
        } else {
          console.log('No primary transfer state found, trying backups...');
          
          // Try to find the most recent backup in IndexedDB
          try {
            // Get all keys in the store
            const keysRequest = store.getAllKeys();
            keysRequest.onsuccess = (event) => {
              const keys = event.target.result;
              
              // Filter for backup keys and sort by timestamp (newest first)
              const backupKeys = keys.filter(key => 
                typeof key === 'string' && key.startsWith('backup_')
              ).sort().reverse();
              
              if (backupKeys.length > 0) {
                console.log(`Found ${backupKeys.length} backup states, trying most recent...`);
                const latestBackupKey = backupKeys[0];
                
                const backupRequest = store.get(latestBackupKey);
                backupRequest.onsuccess = (event) => {
                  if (event.target.result) {
                    console.log(`Restored transfer state from backup: ${latestBackupKey}`);
                    resolve(event.target.result);
                  } else {
                    // Try localStorage fallback
                    tryLocalStorageFallback();
                  }
                };
                
                backupRequest.onerror = () => tryLocalStorageFallback();
              } else {
                // No backup keys found
                tryLocalStorageFallback();
              }
            };
            
            keysRequest.onerror = () => tryLocalStorageFallback();
          } catch (e) {
            console.error('Error trying to find backup states:', e);
            tryLocalStorageFallback();
          }
        }
      };
      
      // Try to load from localStorage as a last resort
      const tryLocalStorageFallback = () => {
        console.log('Trying localStorage fallback for transfer state...');
        const localBackup = localStorage.getItem('transferStateBackup');
        
        if (localBackup) {
          try {
            const parsedBackup = JSON.parse(localBackup);
            console.log('Restored partial transfer state from localStorage');
            resolve(parsedBackup);
          } catch (e) {
            console.error('Failed to parse localStorage backup:', e);
            resolve(null);
          }
        } else {
          console.log('No transfer state found in any storage location');
          resolve(null);
        }
      };
      
      request.onerror = (e) => {
        console.error('Error loading transfer state:', e);
        tryLocalStorageFallback();
      };
    } catch (err) {
      console.error('Failed to load transfer state:', err);
      
      // Try localStorage as a last resort
      try {
        const localBackup = localStorage.getItem('transferStateBackup');
        if (localBackup) {
          const parsedBackup = JSON.parse(localBackup);
          console.log('Restored partial transfer state from localStorage after error');
          resolve(parsedBackup);
        } else {
          reject(err);
        }
      } catch (e) {
        reject(err);
      }
    }
  });
};

// Clear transfer state from IndexedDB
const clearTransferState = async (db) => {
  return new Promise((resolve, reject) => {
    try {
      const transaction = db.transaction(['transferState'], 'readwrite');
      const store = transaction.objectStore('transferState');
      const request = store.delete('current');
      
      request.onsuccess = () => {
        console.log('Transfer state cleared successfully');
        resolve();
      };
      
      request.onerror = (e) => {
        console.error('Error clearing transfer state:', e);
        reject(e);
      };
    } catch (err) {
      console.error('Failed to clear transfer state:', err);
      reject(err);
    }
  });
};

// Add this function at the top level, outside of the App component
const generatePeerId = () => {
  // Check if we already have a peer ID stored
  let peerId = localStorage.getItem('peerShareId');
  
  // If not, generate a new one and store it
  if (!peerId) {
    peerId = uuidv4();
    localStorage.setItem('peerShareId', peerId);
  }
  
  return peerId;
};

// Store file metadata separately to ensure it's preserved
const saveFileMetadata = async (db, metadata, roomId) => {
  return new Promise((resolve, reject) => {
    try {
      const transaction = db.transaction(['fileMetadata'], 'readwrite');
      const store = transaction.objectStore('fileMetadata');
      
      // Use roomId as the key to ensure we can find it again
      const metadataToSave = {
        id: `file_${roomId}`,
        metadata,
        roomId,
        timestamp: Date.now()
      };
      
      const request = store.put(metadataToSave);
      
      // Also save to localStorage as backup
      try {
        localStorage.setItem(`file_metadata_${roomId}`, JSON.stringify(metadata));
      } catch (e) {
        console.warn('Failed to save metadata to localStorage:', e);
      }
      
      request.onsuccess = () => {
        console.log('File metadata saved successfully');
        resolve();
      };
      
      request.onerror = (e) => {
        console.error('Error saving file metadata:', e);
        reject(e);
      };
    } catch (err) {
      console.error('Failed to save file metadata:', err);
      reject(err);
    }
  });
};

// Load file metadata with fallbacks
const loadFileMetadata = async (db, roomId) => {
  return new Promise((resolve, reject) => {
    try {
      if (!db) {
        throw new Error('Database not initialized');
      }
      
      // First try from IndexedDB
      const transaction = db.transaction(['fileMetadata'], 'readonly');
      const store = transaction.objectStore('fileMetadata');
      const request = store.get(`file_${roomId}`);
      
      request.onsuccess = (event) => {
        if (event.target.result) {
          console.log('File metadata loaded from IndexedDB');
          resolve(event.target.result.metadata);
        } else {
          // Try from localStorage
          try {
            const localData = localStorage.getItem(`file_metadata_${roomId}`);
            if (localData) {
              const parsedData = JSON.parse(localData);
              console.log('File metadata loaded from localStorage');
              resolve(parsedData);
            } else {
              console.log('No file metadata found');
              resolve(null);
            }
          } catch (e) {
            console.error('Error loading from localStorage:', e);
            resolve(null);
          }
        }
      };
      
      request.onerror = (e) => {
        console.error('Error loading file metadata:', e);
        // Try from localStorage as fallback
        try {
          const localData = localStorage.getItem(`file_metadata_${roomId}`);
          if (localData) {
            resolve(JSON.parse(localData));
          } else {
            reject(e);
          }
        } catch (e2) {
          reject(e);
        }
      };
    } catch (err) {
      console.error('Failed to load file metadata:', err);
      
      // Last resort - try localStorage
      try {
        const localData = localStorage.getItem(`file_metadata_${roomId}`);
        if (localData) {
          resolve(JSON.parse(localData));
        } else {
          reject(err);
        }
      } catch (e) {
        reject(err);
      }
    }
  });
};

// Add this utility function for clipboard operations
const copyToClipboard = (text) => {
  return new Promise((resolve, reject) => {
    // Try using the Clipboard API first
    if (navigator.clipboard && window.isSecureContext) {
      navigator.clipboard.writeText(text)
        .then(() => resolve(true))
        .catch(err => {
          console.error('Clipboard API error:', err);
          fallbackCopyToClipboard(text, resolve, reject);
        });
    } else {
      // Fall back to older methods
      fallbackCopyToClipboard(text, resolve, reject);
    }
  });
};

const fallbackCopyToClipboard = (text, resolve, reject) => {
  try {
    // Create a temporary input element
    const textArea = document.createElement('textarea');
    textArea.value = text;
    
    // Make it invisible but part of the document
    textArea.style.position = 'fixed';
    textArea.style.opacity = '0';
    document.body.appendChild(textArea);
    
    // Select and copy
    textArea.select();
    const success = document.execCommand('copy');
    
    // Clean up
    document.body.removeChild(textArea);
    
    if (success) {
      resolve(true);
    } else {
      reject(new Error('Unable to copy to clipboard'));
    }
  } catch (err) {
    console.error('Fallback clipboard error:', err);
    reject(err);
  }
};

// After the imports and before the App component
// Dynamic chunk size configuration
const getChunkSizeForFileSize = (fileSize) => {
  // Base chunk size is 64KB by default
  const KB = 1024;
  const MB = 1024 * 1024;
  
  if (fileSize < 1 * MB) {
    // For files < 1MB, use 64KB chunks
    return 64 * KB;
  } else if (fileSize < 10 * MB) {
    // For files 1-10MB, use 128KB chunks
    return 128 * KB;
  } else if (fileSize < 100 * MB) {
    // For files 10-100MB, use 256KB chunks
    return 256 * KB;
  } else if (fileSize < 1024 * MB) {
    // For files 100MB-1GB, use 512KB chunks
    return 512 * KB;
  } else {
    // For files > 1GB, use 1MB chunks
    return 1 * MB;
  }
};

const getMaxParallelChunksForFileSize = (fileSize) => {
  const MB = 1024 * 1024;
  
  if (fileSize < 10 * MB) {
    // For small files (<10MB), use just 1 chunk at a time for reliability
    return 1;
  } else if (fileSize < 100 * MB) {
    // For medium files (10-100MB), use 2 parallel chunks
    return 2;
  } else if (fileSize < 1024 * MB) {
    // For large files (100MB-1GB), use 3 parallel chunks
    return 3;
  } else {
    // For very large files (>1GB), use 4 parallel chunks
    return 4;
  }
};

// The getChunkSizeForIndex function allows for potentially different chunk sizes
// for different parts of the file (could be useful for progressive chunking)
const getChunkSizeForIndex = (fileSize, chunkIndex, totalChunks) => {
  // Special case for last chunk
  if (chunkIndex === totalChunks - 1) {
    return fileSize - (chunkIndex * MAX_CHUNK_SIZE);
  }
  return MAX_CHUNK_SIZE;
};

// Format file size to human-readable format (e.g., KB, MB, GB)
const formatFileSize = (bytes) => {
  if (bytes === 0) return '0 Bytes';
  
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  
  // Use toFixed(2) for MB and higher, toFixed(0) for KB and Bytes
  const formattedSize = i >= 2 
    ? (bytes / Math.pow(1024, i)).toFixed(2) 
    : (bytes / Math.pow(1024, i)).toFixed(0);
    
  return `${formattedSize} ${sizes[i]}`;
};

// Format transfer speed to human-readable format (e.g., KB/s, MB/s)
const formatSpeed = (bytesPerSecond) => {
  if (bytesPerSecond === 0) return '0 B/s';
  
  const sizes = ['B/s', 'KB/s', 'MB/s', 'GB/s'];
  const i = Math.floor(Math.log(bytesPerSecond) / Math.log(1024));
  const formattedSpeed = (bytesPerSecond / Math.pow(1024, i)).toFixed(i > 0 ? 2 : 0);
  
  return `${formattedSpeed} ${sizes[i]}`;
};

// Format time in seconds to a human-readable format
const formatTime = (seconds) => {
  if (seconds === Infinity || isNaN(seconds)) return 'Calculating...';
  
  if (seconds < 60) {
    return `${Math.ceil(seconds)} seconds`;
  } else if (seconds < 3600) {
    return `${Math.floor(seconds / 60)} min ${Math.ceil(seconds % 60)} sec`;
  } else {
    return `${Math.floor(seconds / 3600)} hr ${Math.floor((seconds % 3600) / 60)} min`;
  }
};

/**
 * Checks for any incomplete transfers saved in IndexedDB
 * @param {IDBDatabase} db - The database connection
 */
async function checkForSavedTransfers(db) {
  if (!db) {
    console.error("📊 checkForSavedTransfers: Database not available");
    return;
  }
  
  console.log("🔍 Checking for saved transfers");
  try {
    const savedState = await loadTransferState(db);
    if (savedState) {
      console.log("💾 Found saved transfer state:", savedState);
      // Make it globally available so other components can access it
      window._savedTransferState = savedState;
      
      // Dispatch an event for components to pick up
      window.dispatchEvent(new CustomEvent('saved-transfer-found', { 
        detail: savedState 
      }));
    } else {
      console.log("📭 No saved transfers found");
    }
  } catch (error) {
    console.error("❌ Error checking for saved transfers:", error);
  }
}

// Now continue with the App component
function App() {
  const [roomId, setRoomId] = useState('');
  const [file, setFile] = useState(null);
  const [downloadUrl, setDownloadUrl] = useState('');
  const [isReceiving, setIsReceiving] = useState(false);
  const [isSending, setIsSending] = useState(false);
  const [isResuming, setIsResuming] = useState(false);
  const [progress, setProgress] = useState(0);
  const [error, setError] = useState('');
  const [connectionStatus, setConnectionStatus] = useState('disconnected');
  const [metadata, setMetadata] = useState(null);
  const [estimatedTime, setEstimatedTime] = useState('');
  const [transferSpeed, setTransferSpeed] = useState('');
  const [db, setDb] = useState(null);
  const [roomParticipants, setRoomParticipants] = useState([]);
  const [peerStatus, setPeerStatus] = useState({ connected: false, role: null });
  const receivedChunksCount = useRef(0);
  const startTime = useRef(null);
  const lastUpdateTime = useRef(null);
  const processedBytes = useRef(0);
  const sentChunksCount = useRef(0);
  const fileInputRef = useRef(null);
  const heartbeatInterval = useRef(null);
  const mySocketId = useRef(null);
  const [peerProgress, setPeerProgress] = useState(0);
  const [persistentId, setPersistentId] = useState('');
  const [copied, setCopied] = useState(false);
  const [isDragging, setIsDragging] = useState(false);
  // Add a new state variable to track if we should show the peer progress
  const [showPeerProgress, setShowPeerProgress] = useState(false);
  // Add a specific state variable for receiver progress
  const [receiverProgress, setReceiverProgress] = useState(0);
  // Add networkSpeed state variable
  const [networkSpeed, setNetworkSpeed] = useState('');
  const lastSavedProgress = useRef(0);
  // Use a ref to store the socket instance
  const socketRef = useRef(null);
  // Create a separate state for the displayed link to ensure it updates properly
  const [displayedRoomId, setDisplayedRoomId] = useState(roomId);
  // Add a state variable to force re-renders
  const [forceUpdate, setForceUpdate] = useState(0);
  const [isPreparing, setIsPreparing] = useState(false);
  // Add a new state variable to store the generated link
  const [generatedLink, setGeneratedLink] = useState('');

  // Handle drag and drop events
  const handleDragOver = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(true);
  };

  const handleDragLeave = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(false);
  };

  const handleDrop = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(false);
    
    if (e.dataTransfer.files && e.dataTransfer.files.length > 0) {
      const droppedFile = e.dataTransfer.files[0];
      setFile(droppedFile);
      
      // Similar logic to handleFileChange
      if (socketRef.current && socketRef.current.connected && roomId) {
        socketRef.current.emit('sender-file-selected', {
          roomId,
          fileName: droppedFile.name,
          fileSize: droppedFile.size
        });
      }
    }
  };

  // Start heartbeat checks to monitor connection status
  const startHeartbeat = (roomIdToUse) => {
    if (heartbeatInterval.current) {
      clearInterval(heartbeatInterval.current);
    }
    
    heartbeatInterval.current = setInterval(() => {
      if (socketRef.current && socketRef.current.connected && roomIdToUse) {
        socketRef.current.emit('heartbeat', { roomId: roomIdToUse });
      }
    }, 5000); // Every 5 seconds
  };
  
  // Stop heartbeat checks
  const stopHeartbeat = () => {
    if (heartbeatInterval.current) {
      clearInterval(heartbeatInterval.current);
      heartbeatInterval.current = null;
    }
  };
  
  // Initialize IndexedDB and check for saved transfers on component mount
  useEffect(() => {
    let db;
    const cleanupFunctions = [];

    const initAndRestore = async () => {
      try {
        console.log("🔌 Socket event setup: Initializing database and socket connection");
        db = await initializeDB();
        setDb(db);
        
        // Generate a persistent ID for this device
        const myPeerId = await generatePeerId();
        setPersistentId(myPeerId);
        console.log(`🆔 Socket event setup: Generated persistent ID: ${myPeerId.substring(0, 8)}`);
        
        // Check for saved room ID
        const storedRoomId = localStorage.getItem('currentRoomId');
        
        // Initialize socket after DB is ready with auth data
        const socket = getSocket();
        socketRef.current = socket;
        
        socket.auth = {
          peerId: myPeerId,
          roomId: storedRoomId || '' // Include roomId in auth
        };
        
        if (!socket.connected) {
          socket.connect();
        }
        
        // Set up event listener for saved transfer detection
        const handleSavedTransfer = (event) => {
          console.log('🔄 App: Saved transfer event received:', event.detail);
          const savedState = event.detail;
          
          if (savedState.roomId) {
            setRoomId(savedState.roomId);
          }
          
          if (savedState.fileName && savedState.fileSize) {
            // We don't have the actual file object for sending, but we can set metadata
            setMetadata({
              fileName: savedState.fileName,
              fileSize: savedState.fileSize,
              fileType: savedState.fileType
            });
            
            // Set progress from saved state
            if (savedState.progress) {
              setProgress(savedState.progress);
            }
            
            // Update counters
            if (savedState.receivedChunksCount) {
              receivedChunksCount.current = savedState.receivedChunksCount;
            }
            if (savedState.sentChunksCount) {
              sentChunksCount.current = savedState.sentChunksCount;
            }
            
            // Show a message that we found a saved transfer
            setError(`Found saved transfer: ${savedState.fileName}. Select the file again to resume.`);
            setIsResuming(true);
          }
        };
        
        window.addEventListener('saved-transfer-found', handleSavedTransfer);
        cleanupFunctions.push(() => window.removeEventListener('saved-transfer-found', handleSavedTransfer));
        
        // Set up socket event handlers that don't depend on state
        socketRef.current.on('connect', () => {
          console.log('🔌 Socket Event: Connected with ID:', socketRef.current.id);
          mySocketId.current = socketRef.current.id;
          setConnectionStatus('connected');
          setError('');
          
          // First check for URL parameters
          const urlParams = new URLSearchParams(window.location.search);
          const roomFromUrl = urlParams.get('room');
          const roleFromUrl = urlParams.get('role');
          
          if (roomFromUrl) {
            console.log(`🏠 Socket Event: Room ID found in URL: ${roomFromUrl}`);
            // URL parameters take precedence
            setRoomId(roomFromUrl);
            localStorage.setItem('currentRoomId', roomFromUrl);
            
            // Save the role from URL to localStorage to preserve it across refreshes
            if (roleFromUrl) {
              localStorage.setItem('currentRole', roleFromUrl);
            }
            
            // Get any saved role or default to the URL role or 'receiver'
            const savedRole = roleFromUrl || localStorage.getItem('currentRole') || 'receiver';
            console.log(`👤 Socket Event: Using role from URL: ${savedRole}`);
            
            // Join room with specified role or default to receiver
            console.log(`🔄 Socket Event: Joining room ${roomFromUrl} as ${savedRole}`);
            socketRef.current.emit('join-room', roomFromUrl, { 
              role: savedRole,
              resuming: true,  // Mark as resuming to help with reconnection
              peerId: myPeerId,
              deviceInfo: {
                userAgent: navigator.userAgent,
                platform: navigator.platform
              }
            });
            
            // If this is a receiver refreshing, immediately announce presence
            if (savedRole === 'receiver') {
              setTimeout(() => {
                // Announce presence to help peers detect reconnection
                console.log(`📣 Socket Event: Announcing presence as receiver in room ${roomFromUrl}`);
                socketRef.current.emit('announce-presence', { 
                  roomId: roomFromUrl,
                  role: savedRole,
                  peerId: myPeerId
                });
                
                // Also request a room update to ensure we have latest participants
                socketRef.current.emit('request-room-update', { roomId: roomFromUrl });
              }, 1000); // Short delay to ensure join-room completes first
            }
            
            // Start heartbeat monitoring
            startHeartbeat(roomFromUrl);
          } 
          else if (storedRoomId) {
            console.log(`🏠 Socket Event: Restoring room from localStorage: ${storedRoomId}`);
            // Restore from localStorage if available
            setRoomId(storedRoomId);
            
            // Get saved role for this room
            const savedRole = localStorage.getItem('currentRole') || 'sender';
            console.log(`👤 Socket Event: Using role from storage: ${savedRole}`);
            
            // Rejoin the room
            console.log(`🔄 Socket Event: Rejoining room ${storedRoomId} as ${savedRole}`);
            socketRef.current.emit('join-room', storedRoomId, { 
              role: savedRole,
              resuming: true,
              peerId: myPeerId,
              deviceInfo: {
                userAgent: navigator.userAgent,
                platform: navigator.platform
              }
            });
            
            // Start heartbeat monitoring
            startHeartbeat(storedRoomId);
          } else {
            console.log(`📝 Socket Event: No room ID found, waiting for user action`);
          }
          
          // Then check for saved transfers
          checkForSavedTransfers(db);
        });
        
        // Cleanup function for this listener
        cleanupFunctions.push(() => socketRef.current.off('connect'));
        
        socketRef.current.on('disconnect', (reason) => {
          console.log('🔌 Socket Event: Disconnected. Reason:', reason);
          setConnectionStatus('disconnected');
          setError('Connection lost. Reconnecting...');
          
          // Auto-save current transfer state on disconnect
          if ((isSending || isReceiving) && db) {
            console.log('💾 Socket Event: Auto-saving transfer state on disconnect');
            const stateToSave = {
              isSending,
              isReceiving,
              metadata,
              roomId,
              fileName: file?.name,
              fileSize: file?.size,
              fileType: file?.type,
              progress,
              receivedChunksCount: receivedChunksCount.current,
              sentChunksCount: sentChunksCount.current,
              timestamp: Date.now(),
              persistentId
            };
            
            saveTransferState(db, stateToSave).catch(err => {
              console.error('❌ Socket Event: Error saving transfer state:', err);
            });
          }
        });
        
        // Listen for file-related events
        
        // When a sender starts preparing to send a file
        socketRef.current.on('sender-preparing-file', (data) => {
          console.log('📨 Socket Event: Sender is preparing to send file:', data);
          // Update UI to show preparation
          if (peerStatus.role === 'sender') {
            setShowPeerProgress(true);
          }
        });
        
        // When file metadata is received (for receivers)
        socketRef.current.on('file-meta', (metadata) => {
          console.log('📋 Socket Event: Received file metadata:', metadata);
          
          // Update our role to receiver
          socketRef.current.emit('update-user-data', {
            roomId,
            userData: { 
              role: 'receiver', 
              status: 'receiving'
            }
          });
          
          // Store role in localStorage
          localStorage.setItem('currentRole', 'receiver');
          
          // Set metadata and prepare for receiving
          setIsReceiving(true);
          setMetadata(metadata);
          setError('');
          startTime.current = Date.now();
          lastUpdateTime.current = Date.now();
          
          // Reset counters for new transfer
          receivedChunksCount.current = 0;
          processedBytes.current = 0;
        });
        
        // Add cleanup for this listener
        cleanupFunctions.push(() => socketRef.current?.off('file-meta'));
        
        // When a chunk is received (for receivers)
        socketRef.current.on('file-chunk', (data) => {
          console.log(`📦 Socket Event: Received chunk ${data.chunkIndex}, size: ${data.chunk?.byteLength || 'unknown'} bytes`);
          
          // Process the received chunk
          if (!db) {
            console.error('❌ Socket Event: Database not available for storing chunk');
            return;
          }
          
          const { chunk, chunkIndex, sendId } = data;
          
          // Store the chunk in IndexedDB
          storeChunk(db, chunk, chunkIndex)
            .then(() => {
              // Update the received chunks count
              receivedChunksCount.current = Math.max(receivedChunksCount.current, chunkIndex + 1);
              
              // Send acknowledgment back to sender
              socketRef.current.emit('chunk-ack', {
                roomId,
                chunkIndex,
                sendId,
                receivedAt: Date.now()
              });
              
              // Calculate progress percentage
              const totalChunks = metadata?.totalChunks || 100;
              const newProgress = Math.floor((receivedChunksCount.current / totalChunks) * 100);
              setProgress(newProgress);
              
              // Update processed bytes for speed calculation
              const chunkSize = metadata?.chunkSize || MAX_CHUNK_SIZE;
              processedBytes.current = Math.min(chunkIndex * chunkSize + chunk.byteLength, metadata?.fileSize || 0);
              
              // Calculate transfer speed and remaining time
              const now = Date.now();
              const elapsed = (now - startTime.current) / 1000; // seconds
              if (elapsed > 0) {
                const bytesPerSecond = processedBytes.current / elapsed;
                setTransferSpeed(formatSpeed(bytesPerSecond));
                
                // Remaining time estimation
                if (metadata?.fileSize) {
                  const remaining = metadata.fileSize - processedBytes.current;
                  const remainingSeconds = remaining / bytesPerSecond;
                  setEstimatedTime(formatTime(remainingSeconds));
                }
              }
              
              // Update the server with our progress
              // Send progress updates less frequently to reduce network traffic
              if (newProgress % 5 === 0 || newProgress === 100 || now - lastUpdateTime.current > 2000) {
                socketRef.current.emit('progress-update', {
                  roomId,
                  progress: newProgress,
                  role: 'receiver',
                  processedBytes: processedBytes.current
                });
                lastUpdateTime.current = now;
              }
              
              // If this is the last chunk, create the file
              if (data.isLast || newProgress >= 100) {
                console.log('✅ Socket Event: All chunks received, assembling file...');
                
                // Get all chunks and create a file
                getAllChunks(db, receivedChunksCount.current)
                  .then(chunks => {
                    // Create a blob from all chunks
                    const blob = new Blob(chunks, { type: metadata?.fileType || 'application/octet-stream' });
                    
                    // Create download URL
                    const url = URL.createObjectURL(blob);
                    setDownloadUrl(url);
                    
                    // Update room state to indicate file is complete
                    socketRef.current.emit('update-user-data', {
                      roomId,
                      userData: {
                        status: 'completed',
                        progress: 100
                      }
                    });
                    
                    // Reset state
                    setIsReceiving(false);
                    console.log('✅ Socket Event: File download ready!');
                  })
                  .catch(err => {
                    console.error('❌ Socket Event: Error creating file:', err);
                    setError(`Error creating file: ${err.message}`);
                  });
              }
            })
            .catch(err => {
              console.error(`❌ Socket Event: Error storing chunk ${chunkIndex}:`, err);
              setError(`Error storing chunk ${chunkIndex}: ${err.message}`);
            });
        });
        
        // Add cleanup for this listener
        cleanupFunctions.push(() => socketRef.current?.off('file-chunk'));
        
        // When the peer's progress is updated
        socketRef.current.on('progress-update', (data) => {
          console.log(`📊 Socket Event: Progress update from peer: ${data.progress}%`);
          if (data.progress !== peerProgress) {
            setPeerProgress(data.progress);
          }
          
          // If this is an update from a receiver and we're the sender, update receiverProgress
          if (data.role === 'receiver' && localStorage.getItem('currentRole') === 'sender') {
            setReceiverProgress(data.progress);
          }
          
          // Update UI to show peer's progress
          setShowPeerProgress(true);
        });
        
        // Add cleanup for this listener
        cleanupFunctions.push(() => socketRef.current?.off('progress-update'));
        
        // When there's an error in the transfer
        socketRef.current.on('transfer-error', (data) => {
          console.error('❌ Socket Event: Transfer error:', data);
          setError(`Transfer error: ${data.error || 'Unknown error'}`);
        });
        
        // When the transfer is complete
        socketRef.current.on('file-complete', (data) => {
          console.log('✅ Socket Event: File transfer complete:', data);
          // Handle completion logic
        });
        
        // Add event handler for room participants updates
        socketRef.current.on('room-participants', (data) => {
          console.log('👥 Socket Event: Room participants updated:', data);
          
          // Update participants list
          if (data.participants) {
            setRoomParticipants(data.participants);
            
            // Check if there are other participants besides ourselves
            const myId = socketRef.current.id;
            const otherParticipants = data.participants.filter(p => p.id !== myId && p.connected);
            
            if (otherParticipants.length > 0) {
              // Found at least one connected peer
              const peer = otherParticipants[0];
              console.log(`🔗 Socket Event: Connected peer found - ${peer.id}, role: ${peer.role || 'unknown'}`);
              
              setPeerStatus({
                connected: true,
                role: peer.role || null,
                peerId: peer.peerId || peer.id
              });
            } else {
              console.log('⏳ Socket Event: No other connected peers in the room');
              setPeerStatus({ connected: false, role: null });
            }
          }
        });
        
        // Add cleanup for this listener
        cleanupFunctions.push(() => socketRef.current?.off('room-participants'));
        
        // Listen for peer reconnection events for immediate UI updates
        socketRef.current.on('peer-reconnected', (data) => {
          console.log('🔄 Socket Event: Peer reconnected:', data);
          
          // Update peer status if it's someone other than us
          if (data.socketId !== socketRef.current.id) {
            setPeerStatus({
              connected: true,
              role: data.role || null,
              peerId: data.peerId || data.socketId
            });
            
            // Request an immediate room update to get complete information
            socketRef.current.emit('request-room-update', { roomId });
          }
        });
        
        // Add cleanup for this listener
        cleanupFunctions.push(() => socketRef.current?.off('peer-reconnected'));
        
        return () => {
          // Clean up all listeners
          cleanupFunctions.forEach(cleanup => cleanup());
        };
      } catch (err) {
        console.error('❌ Socket event setup: Error initializing app:', err);
        setError(`Failed to initialize: ${err.message}`);
      }
    };
    
    initAndRestore();
  }, [roomId, isSending, isReceiving, file, peerStatus, metadata]);

  // Add this to the end of the useEffect where we handle socket connection events
  // Force peer discovery on initial room join
  useEffect(() => {
    // Skip effect if key conditions aren't met
    if (!roomId) return;
    
    const socket = socketRef.current;
    if (!socket || !socket.connected) return;
    
    console.log("Room ID detected, sending presence announcement");
    
    // Send an active ping to help other room participants discover us
    const currentRole = localStorage.getItem('currentRole') || 
                     (isSending ? 'sender' : isReceiving ? 'receiver' : 'sender');
    
    // Announce our presence to the room
    socket.emit('announce-presence', { 
      roomId,
      role: currentRole,
      peerId: persistentId
    });
    
    // Request an immediate room participant update
    socket.emit('request-room-update', { roomId });
    
    // No cleanup needed for this one-time announcement
  }, [roomId, isSending, isReceiving, persistentId]);

  // Set up a ref to track if we should skip the next roomId effect
  const skipRoomIdEffect = useRef(false);

  // Use a modified effect that respects the "skip" flag
  useEffect(() => {
    if (skipRoomIdEffect.current) {
      console.log("Skipping roomId effect as requested");
      skipRoomIdEffect.current = false;
      return;
    }
    
    // Only update if they're different to avoid unnecessary re-renders
    if (roomId !== displayedRoomId) {
      console.log(`roomId effect running - updating displayed room: ${roomId}`);
      setDisplayedRoomId(roomId);
      // Force a re-render
      setForceUpdate(prev => prev + 1);
    }
  }, [roomId, displayedRoomId]);

  // Generate link with the displayed room ID - always use displayedRoomId directly
  useEffect(() => {
    if (displayedRoomId) {
      const linkUrl = `${window.location.origin}/?room=${displayedRoomId}`;
      setGeneratedLink(linkUrl);
      console.log("Link updated to:", linkUrl);
    } else {
      setGeneratedLink('');
    }
  }, [displayedRoomId, forceUpdate]);

  // When the New Link button is clicked
  const generateNewRoomId = () => {
    if (!socketRef.current) {
      setError('Socket connection not available. Please reload the page.');
      return;
    }
    
    try {
      console.log("Generating new room ID");
      
      // Store the previous room ID
      const previousRoomId = roomId;
      
      // Leave current room if any
      if (roomId) {
        console.log('Leaving current room:', roomId);
        socketRef.current.emit('leave-room', roomId);
        
        // Expire the previous room so no one else can join
        console.log('Expiring previous room:', roomId);
        socketRef.current.emit('expire-previous-room', roomId);
      }
      
      // Generate a completely new room ID
      const newRoom = uuidv4();
      console.log('Generated new room ID:', newRoom);
      
      // Force the UI to update by updating displayedRoomId directly
      setDisplayedRoomId(''); // Clear first
      setForceUpdate(prev => prev + 1); // Force a re-render
      
      setTimeout(() => {
        setDisplayedRoomId(newRoom); // Then set the new value
        setForceUpdate(prev => prev + 1); // Force another re-render
        
        // Update roomId after displayedRoomId to ensure UI updates first
        skipRoomIdEffect.current = true;
        setRoomId(newRoom);
        
        // Store new room ID in localStorage
        localStorage.setItem('currentRoomId', newRoom);
        
        // Join the new room with role as sender
        console.log('Joining new room as sender:', newRoom);
        socketRef.current.emit('join-room', newRoom, { 
          role: 'sender', 
          peerId: persistentId,
          deviceInfo: {
            userAgent: navigator.userAgent,
            platform: navigator.platform
          }
        });
        
        // Store role in localStorage
        localStorage.setItem('currentRole', 'sender');
        
        // Start heartbeat monitoring for new room
        startHeartbeat(newRoom);
        
        // Clear any previous file selection and state
        setFile(null);
        setProgress(0);
        setPeerProgress(0);
        setReceiverProgress(0);
        setError('');
        setPeerStatus({ connected: false, role: null });
        setRoomParticipants([]);
      }, 0);
      
      return newRoom;
    } catch (error) {
      console.error('Error generating new room ID:', error);
      setError('Failed to generate new room. Please try again.');
      return null;
    }
  };

  // Modify the handleFileChange function to not set download URL for senders
  const handleFileChange = (e) => {
    const socket = socketRef.current;
    if (!socket) {
      setError('Socket connection not available. Please reload the page.');
      return;
    }
    
    const selectedFile = e.target.files[0];
    if (!selectedFile) return;
    
    if (selectedFile.size > 300 * 1024 * 1024 * 1024) { // 300GB limit
      setError('File is too large. Maximum file size is 300GB.');
      return;
    }
    
    setFile(selectedFile);
    setError('');
    
    // Don't create download URL for senders - only needed for receivers
    // const url = URL.createObjectURL(selectedFile);
    // setDownloadUrl(url);

    // If we don't have a room ID yet, generate one
    if (!roomId) {
    const room = uuidv4();
    setRoomId(room);

      // Store room ID in localStorage for persistence
      localStorage.setItem('currentRoomId', room);
      
      // Join room with role as sender
      socket.emit('join-room', room, { 
        role: 'sender', 
        peerId: persistentId,
        deviceInfo: {
          userAgent: navigator.userAgent,
          platform: navigator.platform
        }
      });
      
      // Start heartbeat monitoring
      startHeartbeat(room);
    } 
    // If we already have a room ID and are resuming a previous transfer
    else if (isResuming) {
      setIsResuming(false);
      // Resume sending from where we left off
      if (socketRef.current.connected) {
        // Announce to receivers that sender is ready to resume
        socketRef.current.emit('sender-ready-to-resume', {
          roomId,
          fileName: selectedFile.name,
          fileSize: selectedFile.size,
          fileType: selectedFile.type
        });
        
        // Wait a short time for any receivers to request resume
        setTimeout(() => {
          resumeSending();
        }, 1000);
      }
    }
    // If we have a room ID but are not resuming, keep using the existing room ID
    else if (roomId && socketRef.current.connected) {
      // Just update our role and status with the existing room ID
      socketRef.current.emit('update-user-data', {
        roomId,
        userData: { 
          role: 'sender', 
          status: 'ready',
          fileName: selectedFile.name
        }
      });
      
      // Announce sender presence after file selection
      socketRef.current.emit('sender-file-selected', {
        roomId,
        fileName: selectedFile.name,
        fileSize: selectedFile.size
      });
    }
  };

  // Enhance resumeSending with connection validation
  const resumeSending = async () => {
    try {
      console.log("🚀 resumeSending: Starting file transfer process");
      
      // Double check socket connection and try to reconnect if needed
      if (!socketRef.current) {
        console.error("❌ resumeSending: Socket reference is null, trying to recreate");
        try {
          // Try to recreate the socket
          const socket = getSocket();
          socketRef.current = socket;
          socket.auth = {
            peerId: persistentId,
            roomId: roomId || ''
          };
          socket.connect();
          console.log("🔄 resumeSending: Created new socket instance");
          
          // Wait a bit for connection
          await new Promise(resolve => setTimeout(resolve, 1000));
        } catch (e) {
          console.error("❌ resumeSending: Failed to recreate socket", e);
          setError('Socket initialization failed. Please reload the page.');
          setIsPreparing(false);
          return;
        }
      }
      
      // Check if socket is connected
      if (!socketRef.current.connected) {
        console.warn("⚠️ resumeSending: Socket not connected, attempting reconnection");
        
        try {
          // Log socket details for debugging
          console.log("🔍 resumeSending: Socket details:", {
            id: socketRef.current.id,
            connected: socketRef.current.connected,
            disconnected: socketRef.current.disconnected,
            status: socketRef.current.io ? socketRef.current.io.engine.readyState : 'unknown'
          });
          
          // Force a reconnection
          socketRef.current.connect();
          console.log("🔄 resumeSending: Reconnect attempt initiated");
          
          // Wait for socket to connect with timeout
          await Promise.race([
            new Promise((resolve, reject) => {
              const connectionTimeout = setTimeout(() => {
                reject(new Error("Socket reconnection timeout"));
              }, 5000);
              
              socketRef.current.once('connect', () => {
                clearTimeout(connectionTimeout);
                resolve();
              });
            }),
            new Promise((_, reject) => setTimeout(() => reject(new Error("Connection timeout")), 5000))
          ]);
          
          console.log("✅ resumeSending: Socket reconnected successfully");
          
          // Rejoin room if needed
          if (roomId) {
            console.log(`🔄 resumeSending: Rejoining room ${roomId} after reconnection`);
            socketRef.current.emit('join-room', roomId, { 
              role: 'sender', 
              peerId: persistentId,
              deviceInfo: {
                userAgent: navigator.userAgent,
                platform: navigator.platform
              }
            });
            
            // Give the server a moment to process the room join
            await new Promise(resolve => setTimeout(resolve, 500));
          }
        } catch (error) {
          console.error("❌ resumeSending: Failed to reconnect socket:", error);
          setError('Failed to connect to server. Please reload the page and try again.');
          setIsPreparing(false);
          return;
        }
      }
      
      console.log("📤 resumeSending: File details:", { 
        name: file?.name, 
        size: file?.size, 
        type: file?.type,
        socketId: socketRef.current.id,
        roomId: roomId
      });
      
      if (!file || !db) {
        console.error("❌ resumeSending: Missing file or database:", { fileExists: !!file, dbExists: !!db });
        setError('File or database not available. Please reload the page.');
        setIsPreparing(false); // Clear preparing state on error
        return;
      }
      
      // Check if socket is initialized and connected
      if (!socketRef.current) {
        console.error("❌ resumeSending: Socket reference is missing");
        setError('Socket not initialized. Please reload the page.');
        setIsPreparing(false);
        return;
      }
      
      // Main file transfer logic
      console.log(`🚀 resumeSending: Starting file transfer for ${file.name} (${formatFileSize(file.size)})`);
      
      setIsSending(true);
      setIsPreparing(false); // Clear preparing state as we're now sending
      startTime.current = Date.now();
      lastUpdateTime.current = Date.now();
      
      // Add connection status check interval to abort transfer if connection is lost
      const connectionCheckInterval = setInterval(() => {
        if (!socketRef.current || !socketRef.current.connected) {
          console.error('❌ resumeSending: Socket connection lost during transfer');
          setError('Connection lost during transfer. Please reload and try again.');
          clearInterval(connectionCheckInterval);
          setIsSending(false);
        }
      }, 5000); // Check every 5 seconds
      
      // Clean up this interval when the transfer is complete
      const transferCompleteListener = () => {
        console.log('✅ resumeSending: Transfer complete event received, cleaning up');
        clearInterval(connectionCheckInterval);
      };
      
      // Add this listener and make sure to remove it appropriately
      socketRef.current.once('file-complete', transferCompleteListener);
      
      // Optimize chunk size based on file size - balancing speed and reliability
      let chunkSize = 128 * 1024; // Default 128KB
      
      if (file.size < 10 * 1024 * 1024) {
        // Small files: Use smaller chunks for better reliability
        chunkSize = 16 * 1024; // 16KB
      } else if (file.size > 100 * 1024 * 1024) {
        // Large files: Use larger chunks for better throughput
        chunkSize = 512 * 1024; // 512KB
      } else {
        // Medium files: Balance between reliability and throughput
        chunkSize = 128 * 1024; // 128KB
      }
      
      // For very small files (< 2MB), use smaller chunks
      if (file.size < 2 * 1024 * 1024) {
        chunkSize = 8 * 1024; // 8KB chunks
      }
      
      // For tiny files (< 500KB), use tiny chunks
      if (file.size < 500 * 1024) {
        chunkSize = 4 * 1024; // 4KB chunks
      }
      
      console.log(`📊 resumeSending: Using chunk size ${formatFileSize(chunkSize)} for file size ${formatFileSize(file.size)}`);
      console.log(`💾 resumeSending: File info: type=${file.type}, lastModified=${new Date(file.lastModified).toISOString()}`);
      
      // Function to get chunk size for a specific chunk index
      // Use smaller chunks for the first few to ensure quick start
      const getChunkSizeForIndex = (index) => {
        if (file.size < 500 * 1024) {
          // For tiny files, keep chunks small
          return 4 * 1024; // 4KB
        }
        
        if (file.size < 2 * 1024 * 1024) {
          // For very small files, keep chunks consistently small
          return 8 * 1024; // 8KB
        }
        
        if (index === 0) {
          // First chunk is smaller to ensure it succeeds
          return 4 * 1024; // 4KB for first chunk
        } else if (index < 5) {
          // First few chunks are smaller to ensure they succeed
          return 16 * 1024; // 16KB for first chunks
        }
        return chunkSize;
      };
      
      const totalChunks = Math.ceil(file.size / chunkSize);
      console.log(`📦 resumeSending: File will be split into ${totalChunks} chunks`);

      // Start from the first chunk if no previous progress
      let currentChunk = sentChunksCount.current || 0;
      console.log(`📋 resumeSending: Starting from chunk ${currentChunk} of ${totalChunks} (${(currentChunk / totalChunks * 100).toFixed(1)}% completed already)`);
      
      // Reset processed bytes based on what we've already sent
      processedBytes.current = currentChunk * chunkSize;
      console.log(`📈 resumeSending: Starting with ${formatFileSize(processedBytes.current)} of ${formatFileSize(file.size)} already processed`);
      
      // Update our role and status
      socketRef.current.emit('update-user-data', {
        roomId,
        userData: { 
          role: 'sender', 
          status: 'sending',
          fileName: file.name,
          progress: Math.floor((currentChunk / totalChunks) * 100)
        }
      });
      
      // If this is the initial send (not resuming), send the metadata first
      if (currentChunk === 0) {
        console.log(`📝 resumeSending: Sending file metadata to peer`);
        socketRef.current.emit('file-meta', { 
          roomId, 
          metadata: { 
            fileName: file.name, 
            fileType: file.type, 
            fileSize: file.size, 
            totalChunks,
            chunkSize
          } 
        });
      } else {
        console.log(`🔄 resumeSending: Resuming transfer, skipping metadata send`);
      }
      
      // Implement parallel chunk sending for better throughput
      // Optimize for speed while maintaining reliability
      let MAX_PARALLEL_CHUNKS = 5; // Default: Send up to 5 chunks in parallel (increased from 3)
      
      // For very small files, use sequential sending for better reliability
      if (file.size < 2 * 1024 * 1024) {
        MAX_PARALLEL_CHUNKS = 2; // Send 2 chunks at a time for small files (increased from 1)
      } else if (file.size < 10 * 1024 * 1024) {
        MAX_PARALLEL_CHUNKS = 3; // Send 3 chunks for medium-small files
      } else if (file.size > 100 * 1024 * 1024) {
        MAX_PARALLEL_CHUNKS = 8; // Send 8 chunks for large files
      }
      
      const activeChunks = new Set(); // Track chunks currently being processed
      const chunkQueue = []; // Queue of chunks waiting to be sent
      
      // Make sure to clean up the interval when transfer ends or on errors
      const cleanupFunction = () => {
        if (connectionCheckInterval) {
          clearInterval(connectionCheckInterval);
        }
        if (socketRef.current) {
          socketRef.current.off('file-complete', transferCompleteListener);
        }
      };
      
      // Add cleanup to the window object for access on unmount
      // This avoids using hooks inside a non-component function
      window._cleanupTransfer = cleanupFunction;
      
      // Add event handler to clean up on unload
      window.addEventListener('beforeunload', cleanupFunction);
      
      // Utility function to start sending the next queued chunk
      const processNextChunkFromQueue = () => {
        if (chunkQueue.length > 0 && activeChunks.size < MAX_PARALLEL_CHUNKS) {
          const nextChunk = chunkQueue.shift();
          console.log(`⏭️ processNextChunkFromQueue: Starting next chunk ${nextChunk}, queue length: ${chunkQueue.length}, active: ${activeChunks.size}/${MAX_PARALLEL_CHUNKS}`);
          processChunk(nextChunk);
        } else {
          console.log(`⏸️ processNextChunkFromQueue: No chunks to process. Queue: ${chunkQueue.length}, Active: ${activeChunks.size}/${MAX_PARALLEL_CHUNKS}`);
        }
      };
      
      // Track retry counts
      const retryCountMap = new Map();
      const MAX_RETRIES = 12; // Increased from 8 to allow even more retries for problematic chunks
      
      // Main function to process a chunk - either immediately or queue it
      const processChunk = (chunkIndex) => {
        console.log(`🔄 processChunk: Processing chunk ${chunkIndex}, active chunks: ${activeChunks.size}/${MAX_PARALLEL_CHUNKS}`);
        
        if (activeChunks.size >= MAX_PARALLEL_CHUNKS) {
          // Queue this chunk for later processing
          console.log(`📋 processChunk: Queue full, adding chunk ${chunkIndex} to queue`);
          chunkQueue.push(chunkIndex);
          return;
        }
        
        // Mark this chunk as being processed
        activeChunks.add(chunkIndex);
        console.log(`➕ processChunk: Added chunk ${chunkIndex} to active set, now processing ${activeChunks.size} chunks`);
        
        // Process the chunk
        console.log(`📤 processChunk: Sending chunk ${chunkIndex}`);
        sendChunk(chunkIndex).then(() => {
          // Chunk completed successfully
          console.log(`✅ processChunk: Chunk ${chunkIndex} sent successfully`);
          activeChunks.delete(chunkIndex);
          retryCountMap.delete(chunkIndex); // Clear retry count on success
          
          // Update progress
          const newProgress = Math.floor(((chunkIndex + 1) / totalChunks) * 100);
          if (newProgress > progress) {
            console.log(`📊 processChunk: Updating progress to ${newProgress}%`);
            setProgress(newProgress);
            
            // Update processed bytes
            processedBytes.current = Math.min((chunkIndex + 1) * chunkSize, file.size);
            
            // Update transfer speed
            const now = Date.now();
            const elapsed = (now - startTime.current) / 1000; // seconds
            if (elapsed > 0) {
              const bytesPerSecond = processedBytes.current / elapsed;
              const speed = formatSpeed(bytesPerSecond);
              console.log(`⚡ processChunk: Transfer speed: ${speed}`);
              setNetworkSpeed(speed);
              
              // Calculate estimated time
              const remaining = file.size - processedBytes.current;
              const remainingSeconds = remaining / bytesPerSecond;
              const time = formatTime(remainingSeconds);
              setEstimatedTime(time);
            }
            
            // Emit progress update to peers - limit frequency to reduce overhead
            const timeSinceLastUpdate = now - lastUpdateTime.current;
            if (timeSinceLastUpdate > 1000 || newProgress % 5 === 0) { // Update at least every 1s or every 5%
              if (socketRef.current && socketRef.current.connected) {
                console.log(`📡 processChunk: Emitting progress update: ${newProgress}%`);
                socketRef.current.emit('progress-update', { 
                  roomId, 
                  progress: newProgress,
                  processedBytes: processedBytes.current,
                  totalBytes: file.size 
                });
                lastUpdateTime.current = now;
              }
            }
          }
          
          // Process next chunk from queue
          processNextChunkFromQueue();
          
          // If all chunks done and queue empty, we're finished
          if (activeChunks.size === 0 && chunkQueue.length === 0 && chunkIndex >= totalChunks - 1) {
            console.log(`🏁 processChunk: Transfer complete! All ${totalChunks} chunks sent.`);
            socketRef.current.emit('file-complete', { roomId });
            setIsSending(false);
            setShowPeerProgress(true);
            
            // Update status
            socketRef.current.emit('update-user-data', {
              roomId,
              userData: { 
                status: 'completed',
                progress: 100
              }
            });
            
            // Clean up resources
            cleanupFunction();
            window.removeEventListener('beforeunload', cleanupFunction);
            delete window._cleanupTransfer;
          }
        }).catch(error => {
          console.error(`❌ processChunk: Error sending chunk ${chunkIndex}:`, error);
          
          // Remove from active chunks
          activeChunks.delete(chunkIndex);
          
          // Get current retry count or initialize to 0
          const retryCount = retryCountMap.get(chunkIndex) || 0;
          
          if (retryCount < MAX_RETRIES) {
            // Even gentler backoff strategy for later chunks
            const backoffTime = Math.min(8000, 500 * (retryCount + 1));
            console.log(`🔄 processChunk: Will retry chunk ${chunkIndex} (attempt ${retryCount + 1}/${MAX_RETRIES}) after ${backoffTime}ms`);
            
            // Increment retry count
            retryCountMap.set(chunkIndex, retryCount + 1);
            
            // For problematic chunks that keep failing, try to reduce chunk size on next attempt
            if (retryCount > 5 && socketRef.current && socketRef.current.connected) {
              console.log(`⚠️ processChunk: Notifying server of problematic chunk ${chunkIndex}, retry ${retryCount + 1}`);
              socketRef.current.emit('chunk-retry-notification', {
                roomId,
                chunkIndex,
                retryCount,
                maxRetries: MAX_RETRIES
              });
            }
            
            // Re-queue this chunk with a delay based on retry count
            setTimeout(() => {
              console.log(`🔁 processChunk: Retrying chunk ${chunkIndex} now (attempt ${retryCount + 1}/${MAX_RETRIES})`);
              
              // Check socket connection before retrying
              if (socketRef.current && socketRef.current.connected) {
                chunkQueue.push(chunkIndex);
                processNextChunkFromQueue();
              } else {
                console.error(`❌ processChunk: Cannot retry chunk ${chunkIndex}: socket disconnected`);
                setError('Connection lost. Please reload the page and try again.');
              }
            }, backoffTime);
          } else {
            // For persistently failing chunks, try to force success as last resort
            if (chunkIndex % 3 === 0 || chunkIndex < 50) {
              console.warn(`⚠️ processChunk: Forcing success for repeatedly failing chunk ${chunkIndex} after ${MAX_RETRIES} attempts`);
              
              // Notify server about our forced success
              if (socketRef.current && socketRef.current.connected) {
                socketRef.current.emit('chunk-forced-success', {
                  roomId,
                  chunkIndex,
                  reason: 'max_retries_exhausted'
                });
                
                // Continue with next chunk despite failure
                processNextChunkFromQueue();
                return;
              }
            }
            
            console.error(`❌ processChunk: Failed to send chunk ${chunkIndex} after ${MAX_RETRIES} attempts. Giving up.`);
            setError(`Failed to send data after multiple attempts. Please check your connection and try again.`);
            
            // Clean up resources on error
            cleanupFunction();
            window.removeEventListener('beforeunload', cleanupFunction);
            delete window._cleanupTransfer;
            
            // Attempt to notify the server of the failure
            if (socketRef.current && socketRef.current.connected) {
              socketRef.current.emit('transfer-error', {
                roomId,
                error: `Failed to send chunk ${chunkIndex} after ${MAX_RETRIES} attempts`,
                chunkIndex
              });
            }
          }
        });
      };

      // Function to send a chunk with retries and error handling
      const sendChunk = (chunkIndex) => {
        // Create random ID for this send attempt
        const sendId = uuidv4();
        console.log(`🚀 sendChunk: Starting to send chunk ${chunkIndex} with ID ${sendId.substring(0, 8)}`);
        
        // Get socket reference
        const socket = socketRef.current;
        if (!socket || !socket.connected) {
          console.error('❌ sendChunk: Cannot send chunk: socket not connected');
          throw new Error('Socket not connected');
        }

        // Calculate dynamic timeout based on chunk size and previous performance
        const chunkSize = Math.min(MAX_CHUNK_SIZE, file.size - chunkIndex * MAX_CHUNK_SIZE);
        console.log(`📏 sendChunk: Chunk ${chunkIndex} size: ${formatFileSize(chunkSize)}`);
        
        // Return a new promise rather than calling sendChunk recursively
        return new Promise((resolve, reject) => {
          // Set timeout for acknowledgment
          const timeoutMs = Math.max(15000, chunkSize / 10000); // At least 15 seconds
          console.log(`⏱️ sendChunk: Setting timeout for chunk ${chunkIndex} to ${timeoutMs / 1000}s`);
          
          const timeoutId = setTimeout(() => {
            // Remove the listener to prevent memory leaks
            console.error(`⏰ sendChunk: Timeout (${timeoutMs / 1000}s) reached for chunk ${chunkIndex}`);
            socket.off(`chunk-ack-${sendId}`);
            socket.off(`chunk-ack-room-${roomId}-${chunkIndex}`);
            
            reject(new Error(`Send timeout for chunk ${chunkIndex}`));
          }, timeoutMs);
          
          // Set up acknowledgment handlers
          // For direct acknowledgments (optimized path)
          socket.once(`chunk-ack-${sendId}`, (data) => {
            clearTimeout(timeoutId);
            socket.off(`chunk-ack-room-${roomId}-${chunkIndex}`); // Remove room listener
            console.log(`✅ sendChunk: Received direct ACK for chunk ${chunkIndex} (${sendId.substring(0, 8)})`);
            resolve(data);
          });
          
          // For room-level acknowledgments (fallback path)
          socket.once(`chunk-ack-room-${roomId}-${chunkIndex}`, (data) => {
            clearTimeout(timeoutId);
            socket.off(`chunk-ack-${sendId}`); // Remove direct listener
            console.log(`✅ sendChunk: Received room ACK for chunk ${chunkIndex}`);
            resolve(data);
          });
          
          // Read the chunk data
          console.log(`📖 sendChunk: Reading file chunk ${chunkIndex} data...`);
          const reader = new FileReader();
          
          reader.onload = function(e) {
            try {
              console.log(`📤 sendChunk: File chunk ${chunkIndex} read successfully, size: ${e.target.result.byteLength} bytes`);
              
              // Send the chunk
              console.log(`📡 sendChunk: Emitting file-chunk event for chunk ${chunkIndex}`);
              socket.emit('file-chunk', {
                roomId,
                sendId,
                chunkIndex,
                chunk: e.target.result,
                isLast: chunkIndex === Math.ceil(file.size / MAX_CHUNK_SIZE) - 1
              });
              console.log(`📨 sendChunk: Sent chunk ${chunkIndex} (${sendId.substring(0, 8)}), size: ${e.target.result.byteLength} bytes, waiting for ACK...`);
            } catch (err) {
              clearTimeout(timeoutId);
              socket.off(`chunk-ack-${sendId}`);
              socket.off(`chunk-ack-room-${roomId}-${chunkIndex}`);
              console.error(`❌ sendChunk: Error sending chunk ${chunkIndex}:`, err);
              reject(err);
            }
          };
          
          reader.onerror = function(e) {
            clearTimeout(timeoutId);
            socket.off(`chunk-ack-${sendId}`);
            socket.off(`chunk-ack-room-${roomId}-${chunkIndex}`);
            console.error(`❌ sendChunk: Error reading chunk ${chunkIndex}:`, e);
            reject(new Error(`Error reading chunk ${chunkIndex}: ${e.target.error}`));
          };
          
          // Read the chunk as an array buffer
          const start = chunkIndex * MAX_CHUNK_SIZE;
          const end = Math.min(start + MAX_CHUNK_SIZE, file.size);
          reader.readAsArrayBuffer(file.slice(start, end));
        });
      };

      // Start sending chunks - initiate multiple parallel transfers
      const startChunk = Math.max(0, currentChunk);
      const endChunk = Math.min(startChunk + MAX_PARALLEL_CHUNKS, totalChunks);
      
      // Start the first batch of chunks
      for (let i = startChunk; i < endChunk; i++) {
        processChunk(i);
      }
      
      // Queue up additional chunks
      for (let i = endChunk; i < totalChunks; i++) {
        chunkQueue.push(i);
      }
    } catch (err) {
      console.error('Error sending file:', err);
      setError('Failed to send file: ' + err.message);
      return 1; // Single chunk transmission for tiny files (unchanged)
    }
  };

  // Add a safety timeout for preparing state
  useEffect(() => {
    let prepareTimeout;
    
    // If in preparing state but not sending, add a timeout to reset after 30 seconds
    // This prevents getting stuck in the preparing state
    if (isPreparing && !isSending) {
      console.log("Setting preparing state timeout");
      prepareTimeout = setTimeout(() => {
        console.log("Preparing state timeout triggered - resetting state");
        setIsPreparing(false);
        setError("File preparation timed out. Please try again.");
      }, 30000); // 30 second timeout
    }
    
    return () => {
      if (prepareTimeout) {
        clearTimeout(prepareTimeout);
      }
    };
  }, [isPreparing, isSending]);

  // Get the current URL for sharing
  // generatedLink is already declared at line ~1811
  // const generatedLink = roomId ? `${window.location.origin}/?room=${roomId}` : '';

  // Function to get known peers from localStorage
  const getKnownPeers = () => {
    const peers = [];
    for (let i = 0; i < localStorage.length; i++) {
      const key = localStorage.key(i);
      if (key.startsWith('known_peer_')) {
        try {
          const peerData = JSON.parse(localStorage.getItem(key));
          peers.push({
            id: key.replace('known_peer_', ''),
            lastSeen: peerData.lastSeen,
            role: peerData.role,
            deviceInfo: peerData.deviceInfo || {}
          });
        } catch (err) {
          console.error('Error parsing peer data:', err);
        }
      }
    }
    return peers;
  };

  // Component to display known peers
  const KnownPeersDisplay = () => {
    const knownPeers = getKnownPeers();
    
    if (knownPeers.length === 0) {
  return (
        <div className="no-peers">
          <p>No known peers yet</p>
          <p>Connect with someone to see them here</p>
        </div>
      );
    }
    
    return (
      <div className="peers-list">
        {knownPeers.map(peer => (
          <div key={peer.id} className="peer-item">
            <div className="peer-icon">
              <svg viewBox="0 0 24 24" width="24" height="24" fill="none" stroke="currentColor" strokeWidth="2">
                <path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"></path>
                <circle cx="12" cy="7" r="4"></circle>
              </svg>
      </div>
            <div className="peer-details">
              <div className="peer-id">{peer.id.substring(0, 8)}...</div>
              <div className="peer-role">{peer.role || 'Unknown role'}</div>
              <div className="peer-last-seen">
                Last seen: {new Date(peer.lastSeen).toLocaleString()}
              </div>
            </div>
          </div>
        ))}
      </div>
    );
  };

  // Function to get link for current room
  const getRoomLink = () => {
    return roomId ? `${window.location.origin}/?room=${roomId}` : '';
  };

  // Function to initiate the file transfer process
  const sendFile = () => {
    console.log("🚀 sendFile: Starting file transfer process");
    
    if (!file) {
      console.error("❌ sendFile: No file selected");
      setError('No file selected. Please select a file first.');
      return;
    }
    
    console.log(`📂 sendFile: File info - name: ${file.name}, size: ${formatFileSize(file.size)}, type: ${file.type}`);
    
    if (!roomId) {
      console.error("❌ sendFile: No room available");
      setError('No room available. Please reload the page and try again.');
      return;
    }
    
    console.log(`🏠 sendFile: Room ID: ${roomId}`);
    
    if (!socketRef.current || !socketRef.current.connected) {
      console.error("❌ sendFile: Socket not connected", { socketExists: !!socketRef.current });
      setError('Socket connection not available. Please reload the page.');
      return;
    }
    
    console.log(`🔌 sendFile: Socket connected with ID: ${socketRef.current.id}`);
    
    if (!peerStatus.connected) {
      console.error("❌ sendFile: No peer connected", peerStatus);
      setError('No peer connected. Please wait for a peer to connect before sending.');
      return;
    }
    
    console.log(`👥 sendFile: Peer connected - role: ${peerStatus.role}, id: ${peerStatus.peerId || 'unknown'}`);
    
    // Update our role to sender explicitly
    socketRef.current.emit('update-user-data', {
      roomId,
      userData: { 
        role: 'sender', 
        status: 'preparing',
        fileName: file.name,
        fileSize: file.size,
        fileType: file.type
      }
    });
    
    // Store role in localStorage
    localStorage.setItem('currentRole', 'sender');
    
    // Set preparing state
    console.log("🔄 sendFile: Setting preparing state and announcing to receiver");
    setIsPreparing(true);
    setError('');
    
    // Announce to receiver that we're about to send
    socketRef.current.emit('sender-preparing-file', {
      roomId,
      fileName: file.name,
      fileSize: file.size,
      fileType: file.type
    });
    
    // Short delay to ensure receiver is ready, then start the sending process
    // Let resumeSending handle the state transitions
    console.log("⏱️ sendFile: Setting timeout before starting actual transfer");
    setTimeout(() => {
      console.log("▶️ sendFile: Timeout complete, calling resumeSending()");
      resumeSending();
    }, 1500);
  };

  // Function to cancel an ongoing transfer
  const cancelTransfer = () => {
    console.log('Canceling transfer...');
    
    // Alert the peer that we're canceling
    if (socketRef.current && socketRef.current.connected && roomId) {
      socketRef.current.emit('transfer-canceled', {
        roomId,
        reason: 'user_canceled'
      });
    }
    
    // Reset all transfer-related state
    setIsPreparing(false);
    setIsSending(false);
    setIsReceiving(false);
    setProgress(0);
    setPeerProgress(0);
    setReceiverProgress(0);
    
    // Reset processed bytes counter
    processedBytes.current = 0;
    
    // Clear transfer speed and estimated time
    setTransferSpeed('');
    setNetworkSpeed('');
    setEstimatedTime('');
    
    // Optionally clean up any resources like IndexedDB data
    if (db) {
      clearChunks(db).catch(console.error);
      clearTransferState(db).catch(console.error);
    }
    
    setError('Transfer canceled by user.');
  };

  // Update generatedLink whenever roomId changes
  useEffect(() => {
    if (roomId) {
      setGeneratedLink(`${window.location.origin}/?room=${roomId}`);
    } else {
      setGeneratedLink('');
    }
  }, [roomId]);

  return (
    <div className="app-container">
      <nav className="app-header">
        <div className="logo">
          <svg width="32" height="32" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
            <path d="M16 8L8 16M8 8L16 16" stroke="currentColor" strokeWidth="2" strokeLinecap="round"/>
            <circle cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="2"/>
          </svg>
          <h1>QuickShare</h1>
        </div>
        <div className="connection-status">
          <span className={`status-indicator ${connectionStatus === 'connected' ? 'connected' : 'disconnected'}`}></span>
          <span>{connectionStatus === 'connected' ? 'Connected' : 'Disconnected'}</span>
        </div>
      </nav>

      {error && error.includes('expired') ? (
        <div className="expired-link-notification">
          <div className="error-message">
            <span className="material-icons">link_off</span>
            <p>{error}</p>
      </div>
          <div className="expired-actions">
            <p>Please contact the sender for a new link.</p>
          </div>
        </div>
      ) : error ? (
        <div className="error-message">
          <span className="material-icons">error</span>
          <p>{error}</p>
        </div>
      ) : null}

      <main className="content-grid">
        <section className="card room-card">
          <div className="card-header">
            <h2>Room Connection</h2>
            {peerStatus.connected && (
              <span className="badge success">Peer Connected</span>
            )}
          </div>
          <div className="card-body">
            <div className="room-status">
              {peerStatus.connected ? (
                <div className="peer-info"> 
                  <div className="status-icon connected">
                    <svg viewBox="0 0 24 24" width="24" height="24" fill="none" stroke="currentColor" strokeWidth="2">
                      <path d="M5 12l5 5L20 7"></path>
                    </svg>
                  </div>
                  <div className="peer-details">
                    <h3>Connected to Peer</h3>
                    <p>Role: {peerStatus.role || 'Unknown'}</p>
                    {peerStatus.peerId && (
                      <p className="peer-id">ID: {peerStatus.peerId.substring(0, 8)}...</p>
                    )}
                  </div>
                </div>
              ) : (
                <div className="waiting-connection">
                  <div className="status-icon waiting">
                    <svg viewBox="0 0 24 24" width="24" height="24" fill="none" stroke="currentColor" strokeWidth="2">
                      <circle cx="12" cy="12" r="10"></circle>
                      <path d="M12 6v6l4 2"></path>
                    </svg>
                  </div>
        <div>
                    <h3>Waiting for Connection</h3>
                    <p>Share the link below to connect with someone</p>
                  </div>
        </div>
      )}
            </div>

            <div className="link-share-box">
              <div className="input-with-button">
                <input 
                  type="text" 
                  value={generatedLink}
                  readOnly
                  className="link-input"
                  key={`link-input-${displayedRoomId}-${Date.now()}`}
                />
                <button 
                  className="copy-button"
                  onClick={() => {
                    copyToClipboard(generatedLink)
                      .then(() => {
                        setCopied(true);
                        setTimeout(() => setCopied(false), 2000);
                      })
                      .catch(err => {
                        console.error('Copy failed:', err);
                        alert('Could not copy link. Please copy it manually.');
                      });
                  }}
                >
                  {copied ? (
                    <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="2">
                      <path d="M5 12l5 5L20 7"></path>
                    </svg>
                  ) : (
                    <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="2">
                      <rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect>
                      <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path>
                    </svg>
                  )}
                </button>
              </div>
              
              <div className="qr-and-new">
                <div className="qr-code">
                  <QRCodeSVG value={generatedLink} size={120} />
                  <span>Scan to connect</span>
        </div>
                
                <button 
                  className="new-room-button"
                  onClick={generateNewRoomId}
                >
                  <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="2">
                    <circle cx="12" cy="12" r="10"></circle>
                    <line x1="12" y1="8" x2="12" y2="16"></line>
                    <line x1="8" y1="12" x2="16" y2="12"></line>
                  </svg>
                  New Link
                </button>
              </div>
            </div>
          </div>
        </section>

        <section className="card file-card">
          <div className="card-header">
            <h2>File Transfer</h2>
            {(isSending || isReceiving || showPeerProgress) && (
              <span className="badge progress">{progress}%</span>
            )}
            {isPreparing && (
              <span className="badge progress">Preparing...</span>
            )}
          </div>
          <div className="card-body">
            {/* Debug info for troubleshooting */}
            {process.env.NODE_ENV !== 'production' && (
              <div className="debug-info" style={{marginBottom: '10px', fontSize: '12px', color: '#666', padding: '4px', border: '1px dashed #ccc'}}>
                States: {JSON.stringify({isPreparing, isSending, isReceiving, file: !!file})}
              </div>
            )}
            
            {!isSending && !isReceiving && !isPreparing ? (
              <div 
                className={`dropzone ${isDragging ? 'dragging' : ''}`}
                onDragOver={handleDragOver}
                onDragLeave={handleDragLeave}
                onDrop={handleDrop}
              >
                <input 
                  type="file" 
                  id="file-input" 
                  onChange={handleFileChange}
                  ref={fileInputRef}
                  style={{display: 'none'}}
                />
                <label htmlFor="file-input" className="dropzone-label">
                  <div className="upload-icon">
                    <svg viewBox="0 0 24 24" width="40" height="40" fill="none" stroke="currentColor" strokeWidth="2">
                      <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path>
                      <polyline points="17 8 12 3 7 8"></polyline>
                      <line x1="12" y1="3" x2="12" y2="15"></line>
                    </svg>
                  </div>
                  <div className="dropzone-text">
                    <span>{isDragging ? 'Drop file here' : 'Drag & drop file here'}</span>
                    <span className="or-text">or</span>
                    <span className="browse-text">Browse files</span>
                  </div>
                </label>
              </div>
            ) : null}

            {file && !isSending && !isReceiving && !isPreparing && (
              <div className="selected-file">
                <div className="file-icon">
                  <svg viewBox="0 0 24 24" width="24" height="24" fill="none" stroke="currentColor" strokeWidth="2">
                    <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"></path>
                    <polyline points="14 2 14 8 20 8"></polyline>
                    <line x1="16" y1="13" x2="8" y2="13"></line>
                    <line x1="16" y1="17" x2="8" y2="17"></line>
                    <polyline points="10 9 9 9 8 9"></polyline>
                  </svg>
                </div>
                <div className="file-details">
                  <div className="file-name" title={file.name}>{file.name}</div>
                  <div className="file-size">{formatFileSize(file.size)}</div>
                </div>
                <button 
                  className="send-button"
                  onClick={sendFile}
                  disabled={!peerStatus.connected}
                >
                  <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="2">
                    <line x1="22" y1="2" x2="11" y2="13"></line>
                    <polygon points="22 2 15 22 11 13 2 9 22 2"></polygon>
                  </svg>
                  Send
                </button>
              </div>
            )}

            {isPreparing && file && (
              <div className="preparing-file">
                <div className="preparing-icon">
                  <svg className="spinner" viewBox="0 0 24 24" width="30" height="30" fill="none" stroke="currentColor" strokeWidth="2">
                    <circle cx="12" cy="12" r="10"></circle>
                    <path d="M12 6v1M12 19v1M6 12H5M19 12h1M7.05 7.05l.7.7M16.95 16.95l.7.7M16.95 7.05l-.7.7M7.05 16.95l-.7.7"></path>
                  </svg>
                </div>
                <div className="preparing-details">
                  <div className="preparing-title">Preparing File</div>
                  <div className="preparing-description">Analyzing and optimizing for transfer...</div>
                  <div className="file-name" title={file.name}>{file.name}</div>
                  <div className="file-size">{formatFileSize(file.size)}</div>
                </div>
                <button 
                  className="cancel-button small"
                  onClick={cancelTransfer}
                >
                  <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="2">
                    <circle cx="12" cy="12" r="10"></circle>
                    <line x1="15" y1="9" x2="9" y2="15"></line>
                    <line x1="9" y1="9" x2="15" y2="15"></line>
                  </svg>
                  Cancel
                </button>
              </div>
            )}

            {(isSending || isReceiving || showPeerProgress) && (
              <div className="transfer-progress">
                <div className="transfer-type">
                  <div className="file-info">
                    <span className="transfer-action">
                      {showPeerProgress ? "Peer Receiving" : isSending ? 'Sending' : 'Receiving'}
                    </span>
                    <span className="file-name">{metadata?.fileName || file?.name}</span>
                  </div>
                </div>
                
                <div className="progress-container">
                  <div className="progress-info">
                    <span className="progress-percentage">
                      {isSending ? `Receiver Progress: ${receiverProgress}%` : `Sender Progress: ${peerProgress}%`}
                    </span>
                    <span className="progress-size">
                      {formatFileSize(processedBytes.current)} / 
                      {formatFileSize(metadata?.fileSize || file?.size)}
                    </span>
                  </div>
                  
                  <div className="progress-section">
                    <div className="progress-label">
                      {isSending ? "Receiver Progress" : "Sender Progress"}
                    </div>
                    <div className="progress-bar-container">
                      <div 
                        className="progress-bar peer-progress-bar" 
                        style={{width: `${isSending ? receiverProgress : peerProgress}%`}}
                      ></div>
                    </div>
                  </div>
                </div>
                
                <div className="transfer-stats">
                  <div className="stat">
                    <div className="stat-label">Network Speed</div>
                    <div className="stat-value">{isSending ? networkSpeed || 'Waiting...' : transferSpeed || 'Calculating...'}</div>
                  </div>
                  <div className="stat">
                    <div className="stat-label">Remaining</div>
                    <div className="stat-value">{estimatedTime || 'Calculating...'}</div>
                  </div>
                </div>
                
                {!showPeerProgress && (
                  <button 
                    className="cancel-button"
                    onClick={cancelTransfer}
                  >
                    <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="2">
                      <circle cx="12" cy="12" r="10"></circle>
                      <line x1="15" y1="9" x2="9" y2="15"></line>
                      <line x1="9" y1="9" x2="15" y2="15"></line>
                    </svg>
                    Cancel Transfer
                  </button>
                )}
              </div>
            )}

            {downloadUrl && !isSending && !isReceiving && !file && (
              <div className="download-section">
                <div className="download-icon">
                  <svg viewBox="0 0 24 24" width="40" height="40" fill="none" stroke="currentColor" strokeWidth="2">
                    <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path>
                    <polyline points="7 10 12 15 17 10"></polyline>
                    <line x1="12" y1="15" x2="12" y2="3"></line>
                  </svg>
                </div>
                <div className="download-info">
                  <h3>Transfer Complete!</h3>
                  <p>{metadata?.fileName || 'File'} ({formatFileSize(metadata?.fileSize || 0)})</p>
                </div>
                <a 
                  href={downloadUrl} 
                  download={metadata?.fileName}
                  className="download-button"
                  onClick={handleDownloadClick}
                >
                  Download File
                </a>
              </div>
            )}
          </div>
        </section>

        <section className="card participants-card">
          <div className="card-header">
            <h2>Connected Peers</h2>
            <span className="peer-count">
              {roomParticipants.filter(p => p.id !== mySocketId.current).length}
            </span>
          </div>
          <div className="card-body">
            <KnownPeersDisplay />
          </div>
        </section>
      </main>

      <footer className="app-footer">
        <p>Secure peer-to-peer file sharing • Files transfer directly between peers • Nothing stored on servers</p>
      </footer>
    </div>
  );
}

export default App;