// প্রয়োজনীয় প্যাকেজ ইম্পোর্ট
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

// এক্সপ্রেস অ্যাপ বানানো
const app = express();
app.use(cors());

// HTTP সার্ভার এবং Socket.io সার্ভার তৈরি
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST']
  },
  maxHttpBufferSize: 5e9, // 5GB buffer for very large file transfers
  pingTimeout: 180000, // 3 minutes (reduced from 5 minutes for faster error detection)
  pingInterval: 10000, // 10 seconds (reduced from 25 seconds for more responsive connections)
  // Additional optimizations
  perMessageDeflate: false, // Disable compression for better performance
  httpCompression: false, // Disable HTTP compression for better performance
  upgradeTimeout: 30000, // Increase upgrade timeout for better reliability
  allowUpgrades: true, // Allow transport upgrades
  transports: ['websocket', 'polling'], // Prefer websocket, fall back to polling
  connectTimeout: 45000 // Increase connection timeout to 45 seconds
});

// Active rooms tracking
const activeRooms = new Set();

// Track expired room IDs
const expiredRooms = new Map();

// Track transfer states for resuming
const transferStates = new Map();

// Track participants in each room with persistent IDs
const roomParticipants = new Map();

// Map persistent IDs to socket IDs
const persistentIdMap = new Map();

// Connection monitoring
setInterval(() => {
  console.log(`Active connections: ${io.engine.clientsCount}, Active rooms: ${activeRooms.size}`);
  
  // Clean up old transfer states (older than 24 hours)
  const now = Date.now();
  for (const [roomId, state] of transferStates.entries()) {
    if (now - state.lastUpdated > 24 * 60 * 60 * 1000) {
      transferStates.delete(roomId);
      console.log(`Cleaned up stale transfer state for room ${roomId}`);
    }
  }
  
  // Clean up expired rooms older than 7 days
  for (const [roomId, timestamp] of expiredRooms.entries()) {
    if (now - timestamp > 7 * 24 * 60 * 60 * 1000) {
      expiredRooms.delete(roomId);
      console.log(`Removed expired room ${roomId} from tracking after 7 days`);
    }
  }
}, 30000);

// Helper to get room participants
const getRoomParticipants = (roomId) => {
  if (!roomParticipants.has(roomId)) {
    roomParticipants.set(roomId, new Map());
  }
  return roomParticipants.get(roomId);
};

// Helper to update room participants and emit status updates
const updateRoomParticipants = (roomId, socketId, isConnected, userData = {}) => {
  const participants = getRoomParticipants(roomId);
  
  // Extract persistent ID if available
  const persistentId = userData.peerId || socketId;
  
  if (isConnected) {
    // Add participant or update existing one
    let participantData;
    
    // Check if we have an existing participant with this persistent ID
    const existingEntries = Array.from(participants.entries());
    const existingEntry = existingEntries.find(([_, data]) => data.peerId === persistentId);
    
    if (existingEntry) {
      // Update existing participant with new socket ID
      const [oldKey, oldData] = existingEntry;
      if (oldKey !== socketId) {
        // Remove old entry
        participants.delete(oldKey);
        console.log(`Updated socket mapping for persistent ID ${persistentId}: ${oldKey} -> ${socketId}`);
      }
      
      // Keep existing data but update with new values
      participantData = {
        ...oldData,
        socketId: socketId,
        connected: true,
        lastSeen: Date.now(),
        ...userData
      };
    } else {
      // New participant
      participantData = {
        socketId: socketId,
        peerId: persistentId,
        connected: true,
        lastSeen: Date.now(),
        ...userData
      };
    }
    
    // Update the maps
    participants.set(socketId, participantData);
    persistentIdMap.set(persistentId, socketId);
  } else {
    // Mark as disconnected but don't remove
    if (participants.has(socketId)) {
      const participant = participants.get(socketId);
      participant.connected = false;
      participant.lastSeen = Date.now();
    }
  }
  
  // Notify everyone in the room about the updated participant list
  io.to(roomId).emit('room-participants', {
    participants: Array.from(participants.entries()).map(([id, data]) => ({
      id,
      peerId: data.peerId,
      ...data
    }))
  });
};

// ইভেন্ট শুনছি
io.on('connection', (socket) => {
  console.log('✅ A user connected', socket.id);
  
  // Increase socket buffer size for better performance with large transfers
  socket.conn.setMaxListeners(20); // Allow more listeners for high-throughput transfers
  
  // Get persistent ID from auth if available
  const persistentId = socket.handshake.auth?.peerId;
  if (persistentId) {
    console.log(`User connected with persistent ID: ${persistentId}`);
    persistentIdMap.set(persistentId, socket.id);
    
    // Check if this peer was in any rooms before
    for (const [roomId, participants] of roomParticipants.entries()) {
      for (const [oldSocketId, data] of participants.entries()) {
        if (data.peerId === persistentId && oldSocketId !== socket.id) {
          // This peer reconnected to a room they were in before
          console.log(`Peer ${persistentId} reconnected to room ${roomId}, updating socket ID: ${oldSocketId} -> ${socket.id}`);
          
          // Join the room again
          socket.join(roomId);
          activeRooms.add(roomId);
          
          // Update participant with new socket ID and mark as connected
          data.socketId = socket.id;
          data.connected = true;
          data.lastSeen = Date.now();
          
          // Update the mapping
          participants.delete(oldSocketId);
          participants.set(socket.id, data);
          
          // Notify room of participant update
          io.to(roomId).emit('room-participants', {
            participants: Array.from(participants.entries()).map(([id, data]) => ({
              id,
              peerId: data.peerId,
              ...data
            }))
          });
          
          // If this peer was involved in a transfer, check if we need to resume
          const state = transferStates.get(roomId);
          if (state) {
            if (state.senderId === oldSocketId) {
              // Update the sender ID in the transfer state
              state.senderId = socket.id;
              
              // Check if someone was waiting for this sender to reconnect
              if (state.waitingReceiverId && state.waitingReceiverLastChunk !== undefined) {
                const receiverSocket = io.sockets.sockets.get(state.waitingReceiverId);
                if (receiverSocket) {
                  // Notify the sender about the waiting receiver
                  socket.emit('resume-sending', {
                    roomId,
                    lastChunk: state.waitingReceiverLastChunk,
                    receiverId: state.waitingReceiverId
                  });
                  
                  // Clear waiting state
                  delete state.waitingReceiverId;
                  delete state.waitingReceiverLastChunk;
                }
              }
            }
          }
        }
      }
    }
  }
  
  // Track client IP for debugging
  const clientIp = socket.handshake.headers['x-forwarded-for'] || socket.handshake.address;
  console.log(`Client connected from: ${clientIp}`);

  // Handle errors to prevent disconnection
  socket.on('error', (error) => {
    console.error(`Socket error for ${socket.id}:`, error);
  });

  // Create a map to track in-progress chunks for this socket
  // This helps avoid duplicate chunk handling
  const inProgressChunks = new Map();
  
  // Add handler for file chunk relay
  socket.on('file-chunk', ({ roomId, chunk, chunkIndex, sendId }) => {
    try {
      // Check if socket is in the room
      if (!socket.rooms.has(roomId)) return;
      
      // Add a size threshold to log only for larger chunks or milestone indices
      if (chunkIndex % 50 === 0 || chunkIndex < 5) {
        console.log(`Relaying chunk ${chunkIndex} (${chunk.byteLength} bytes) in room ${roomId}${sendId ? ` with ID ${sendId}` : ''}`);
      }
      
      // Track chunk in the in-progress map
      inProgressChunks.set(chunkIndex, Date.now());
      
      // Forward to receivers, including the sendId if provided
      socket.to(roomId).emit('file-chunk', { chunk, chunkIndex, sendId });
      
      // Update transfer state
      const state = transferStates.get(roomId);
      if (state) {
        state.lastUpdated = Date.now();
        state.lastChunk = chunkIndex;
      }
    } catch (err) {
      console.error(`Error relaying chunk ${chunkIndex} in room ${roomId}:`, err);
      // If sendId is provided, send error back directly to sender
      if (sendId) {
        socket.emit('chunk-received', {
          chunkIndex,
          success: false,
          error: `Server error: ${err.message}`,
          sendId
        });
        
        // Also send direct acknowledgment with the sendId
        socket.emit(`chunk-received-${sendId}`, 'error');
      }
    }
  });

  // Add a handler for direct chunk acknowledgments
  socket.on(/^chunk-received-.*$/, function(status) {
    try {
      // Extract the sendId from the event name
      const eventName = this.event;
      const sendId = eventName.replace('chunk-received-', '');
      
      // Forward the acknowledgment to the room, allowing the sender to pick it up
      if (sendId) {
        // Find which room this is for based on the sendId format: roomId-chunkIndex-timestamp
        const roomIdPart = sendId.split('-')[0];
        
        if (roomIdPart && socket.rooms.has(roomIdPart)) {
          console.log(`Forwarding direct acknowledgment for sendId ${sendId} in room ${roomIdPart}`);
          socket.to(roomIdPart).emit(eventName, status);
        }
      }
    } catch (err) {
      console.error(`Error handling direct chunk acknowledgment:`, err);
    }
  });

  socket.on('join-room', (roomId, userData = {}) => {
    try {
      // Check if the room is expired
      if (expiredRooms.has(roomId)) {
        console.log(`❌ User ${socket.id} attempted to join expired room: ${roomId}`);
        socket.emit('room-expired', { roomId });
        return;
      }
      
      socket.join(roomId);
      activeRooms.add(roomId);
      console.log(`🚪 User ${socket.id} joined room: ${roomId} as role: ${userData.role || 'unknown'}`);
      
      // Store the persistent ID if provided
      const persistentId = userData.peerId || socket.id;
      if (persistentId) {
        persistentIdMap.set(persistentId, socket.id);
        userData.peerId = persistentId;
      }
      
      // Check if this is a reconnecting user with a known peer ID
      let isReconnection = false;
      const participants = getRoomParticipants(roomId);
      
      // Look through existing participants to find a match by peer ID
      for (const [existingId, data] of participants.entries()) {
        if (data.peerId === persistentId && existingId !== socket.id) {
          console.log(`Found reconnecting peer: ${persistentId}, old socket: ${existingId}, new socket: ${socket.id}`);
          
          // This is a reconnection - preserve their data but update the socket ID
          const updatedData = {
            ...data,
            socketId: socket.id,
            connected: true,
            lastSeen: Date.now(),
            ...userData  // Allow updates from the new connection
          };
          
          // Remove old socket entry and add the new one
          participants.delete(existingId);
          participants.set(socket.id, updatedData);
          isReconnection = true;
          
          // If this is a receiver reconnecting, handle specially
          if (updatedData.role === 'receiver' && userData.resuming) {
            // Immediately notify everyone about reconnection
            socket.to(roomId).emit('peer-reconnected', {
              socketId: socket.id,
              peerId: persistentId,
              role: 'receiver'
            });
          }
          
          break;
        }
      }
      
      // If not a reconnection, add as new participant
      if (!isReconnection) {
        // Add to participants with the regular method
        updateRoomParticipants(roomId, socket.id, true, userData);
      } else {
        // For reconnections, we've already updated participants above,
        // but need to emit the updated list to everyone
        io.to(roomId).emit('room-participants', {
          participants: Array.from(participants.entries()).map(([id, data]) => ({
            id,
            peerId: data.peerId,
            ...data
          }))
        });
      }
      
      // If there's a saved transfer state for this room, let the new client know
      if (transferStates.has(roomId)) {
        console.log(`Room ${roomId} has a saved transfer state, informing new user`);
        // Don't send the actual state, just inform that there's a transfer in progress
        socket.emit('transfer-in-progress', { roomId });
      }
    } catch (err) {
      console.error(`Error joining room ${roomId}:`, err);
    }
  });

  // Update user data (role, status, etc.)
  socket.on('update-user-data', ({ roomId, userData }) => {
    try {
      const participants = getRoomParticipants(roomId);
      if (participants.has(socket.id)) {
        // Update user data
        const currentData = participants.get(socket.id);
        participants.set(socket.id, { 
          ...currentData,
          ...userData,
          lastUpdated: Date.now()
        });
        
        // Notify everyone in the room
        io.to(roomId).emit('room-participants', {
          participants: Array.from(participants.entries()).map(([id, data]) => ({
            id,
            ...data
          }))
        });
      }
    } catch (err) {
      console.error(`Error updating user data in room ${roomId}:`, err);
    }
  });

  // Send a heartbeat to check if peers are still connected
  socket.on('heartbeat', ({ roomId }) => {
    try {
      // Mark the sender as active
      const participants = getRoomParticipants(roomId);
      if (participants.has(socket.id)) {
        const participant = participants.get(socket.id);
        participant.lastSeen = Date.now();
        participant.connected = true;
      }
      
      // Broadcast a heartbeat request to everyone else in the room
      socket.to(roomId).emit('heartbeat-request', { 
        from: socket.id,
        timestamp: Date.now()
      });
    } catch (err) {
      console.error(`Error sending heartbeat in room ${roomId}:`, err);
    }
  });

  // Respond to heartbeat requests
  socket.on('heartbeat-response', ({ roomId, to }) => {
    try {
      // Find the requester socket and send the response
      const requesterSocket = io.sockets.sockets.get(to);
      if (requesterSocket) {
        requesterSocket.emit('heartbeat-ack', { 
          from: socket.id,
          timestamp: Date.now()
        });
      }
      
      // Update last seen time
      const participants = getRoomParticipants(roomId);
      if (participants.has(socket.id)) {
        const participant = participants.get(socket.id);
        participant.lastSeen = Date.now();
        participant.connected = true;
      }
    } catch (err) {
      console.error(`Error responding to heartbeat in room ${roomId}:`, err);
    }
  });

  socket.on('file-meta', ({ roomId, metadata }) => {
    try {
      console.log(`📄 File metadata received in room ${roomId}:`, metadata.fileName);
      
      // Save metadata for potential resume
      transferStates.set(roomId, {
        metadata,
        lastUpdated: Date.now(),
        senderId: socket.id
      });
      
      // Update user role to sender
      const participants = getRoomParticipants(roomId);
      if (participants.has(socket.id)) {
        const participant = participants.get(socket.id);
        participant.role = 'sender';
        participant.lastUpdated = Date.now();
      }
      
      socket.to(roomId).emit('file-meta', metadata);
    } catch (err) {
      console.error(`Error handling file metadata in room ${roomId}:`, err);
    }
  });

  socket.on('file-complete', ({ roomId }) => {
    try {
      console.log(`✅ File transfer completed in room ${roomId}`);
      
      // Clean up transfer state for completed transfers
      transferStates.delete(roomId);
      
      // Update participant status
      const participants = getRoomParticipants(roomId);
      if (participants.has(socket.id)) {
        const participant = participants.get(socket.id);
        participant.lastSeen = Date.now();
        participant.lastActivity = 'completed-transfer';
      }
      
      socket.to(roomId).emit('file-complete');
    } catch (err) {
      console.error(`Error handling file completion in room ${roomId}:`, err);
    }
  });

  // Handle resume requests from receiving clients
  socket.on('resume-receiving', ({ roomId, lastChunk }) => {
    try {
      console.log(`📋 Resume request from ${socket.id} in room ${roomId}, last chunk: ${lastChunk}`);
      
      // Update participant role to receiver
      const participants = getRoomParticipants(roomId);
      if (participants.has(socket.id)) {
        const participant = participants.get(socket.id);
        participant.role = 'receiver';
        participant.lastActivity = 'requested-resume';
        participant.lastUpdated = Date.now();
        participant.lastChunk = lastChunk;
        participant.connected = true;
      }
      
      // Get the state for this room
      const state = transferStates.get(roomId);
      
      if (state) {
        // Update the state with this receiver's information
        state.waitingReceiverId = socket.id;
        state.waitingReceiverLastChunk = lastChunk;
        state.lastUpdated = Date.now();
        
        // Find the sender socket and request resume
        const senderSocket = io.sockets.sockets.get(state.senderId);
        
        if (senderSocket) {
          console.log(`🔄 Notifying sender ${state.senderId} to resume sending from chunk ${lastChunk + 1}`);
          senderSocket.emit('resume-sending', { 
            roomId, 
            lastChunk,
            receiverId: socket.id
          });
          
          // Also emit an update to all room participants about the resume request
          io.to(roomId).emit('transfer-resuming', {
            resumingFrom: lastChunk + 1,
            receiverId: socket.id,
            senderId: state.senderId
          });
        } else {
          // If sender is not connected, we need to wait for them to reconnect
          console.log(`⚠️ Sender ${state.senderId} not connected, waiting for reconnection`);
          
          // Look for another connected sender in the room based on role
          let foundAlternateSender = false;
          
          for (const [participantId, data] of participants.entries()) {
            if (data.role === 'sender' && data.connected && participantId !== socket.id) {
              console.log(`🔄 Found alternate sender ${participantId}, notifying to resume`);
              
              const alternateSenderSocket = io.sockets.sockets.get(participantId);
              if (alternateSenderSocket) {
                alternateSenderSocket.emit('resume-sending', { 
                  roomId, 
                  lastChunk,
                  receiverId: socket.id
                });
                
                // Update the state with the new sender
                state.senderId = participantId;
                foundAlternateSender = true;
                break;
              }
            }
          }
          
          if (!foundAlternateSender) {
            // If no sender is available, notify the receiver
            socket.emit('waiting-for-sender', { 
              roomId,
              message: 'Waiting for sender to reconnect...'
            });
          }
        }
      } else {
        console.log(`⚠️ No transfer state for room ${roomId}`);
        socket.emit('no-transfer-state', { roomId });
      }
    } catch (err) {
      console.error(`Error handling resume request in room ${roomId}:`, err);
    }
  });

  // Handle room leave events
  socket.on('leave-room', (roomId) => {
    try {
      socket.leave(roomId);
      
      // Update participant status
      updateRoomParticipants(roomId, socket.id, false);
      
      // Check if room is empty
      const room = io.sockets.adapter.rooms.get(roomId);
      if (!room || room.size === 0) {
        activeRooms.delete(roomId);
        console.log(`🚪 Room ${roomId} is now empty and removed`);
        
        // Don't delete transfer state yet in case they reconnect
        // It will be cleaned up by the interval if it's too old
      }
    } catch (err) {
      console.error(`Error leaving room ${roomId}:`, err);
    }
  });

  socket.on('disconnecting', () => {
    try {
      // Get persistent ID if available
      let peerIdToDisconnect = null;
      for (const [peerId, socketId] of persistentIdMap.entries()) {
        if (socketId === socket.id) {
          peerIdToDisconnect = peerId;
          break;
        }
      }
      
      // Get all rooms this socket is in
      const rooms = socket.rooms;
      
      for (const roomId of rooms) {
        // Skip the room that matches the socket ID (default room)
        if (roomId === socket.id) continue;
        
        // Update participants status to disconnected, but don't remove
        updateRoomParticipants(roomId, socket.id, false);
        
        // Check if room will be empty
        const room = io.sockets.adapter.rooms.get(roomId);
        if (!room || room.size <= 1) { // If 1, it's only this socket that's about to disconnect
          activeRooms.delete(roomId);
          console.log(`🚪 Room ${roomId} will be empty after disconnection and removed`);
        }
        
        // Update transfer state to mark the sender as disconnected
        const state = transferStates.get(roomId);
        if (state && state.senderId === socket.id) {
          console.log(`⚠️ Sender ${socket.id} for room ${roomId} disconnected during transfer`);
          state.senderDisconnected = true;
          state.lastUpdated = Date.now();
          
          // Store persistent ID if available for future reconnection
          if (peerIdToDisconnect) {
            state.senderPeerId = peerIdToDisconnect;
          }
        }
      }
      
      // Don't remove the persistent ID mapping on disconnect
      // This allows us to identify the peer when they reconnect
    } catch (err) {
      console.error(`Error handling disconnecting event for ${socket.id}:`, err);
    }
  });

  socket.on('disconnect', (reason) => {
    console.log(`❌ User disconnected (${socket.id}). Reason: ${reason}`);
  });

  // Add this new event handler for progress updates
  socket.on('progress-update', ({ roomId, progress, role, timestamp, speed, speedFormatted }) => {
    try {
      if (!roomId || !socket.rooms.has(roomId)) return;
      
      // Only log infrequent updates to reduce console spam
      if (progress % 10 === 0 || progress === 100) {
        console.log(`Progress update in room ${roomId}: ${progress}% from ${role}`);
      }
      
      // Store latest progress in room state for new joiners
      const participants = getRoomParticipants(roomId);
      if (participants.has(socket.id)) {
        const participant = participants.get(socket.id);
        participant.progress = progress;
        participant.lastProgressUpdate = timestamp;
        if (speed) participant.transferSpeed = speed;
        if (speedFormatted) participant.speedFormatted = speedFormatted;
      }
      
      // Update transfer state
      const state = transferStates.get(roomId);
      if (state) {
        state.lastUpdated = Date.now();
        if (role === 'sender') {
          state.senderProgress = progress;
          if (speed) state.senderSpeed = speed;
        } else if (role === 'receiver') {
          state.receiverProgress = progress;
          if (speed) state.receiverSpeed = speed;
        }
      }
      
      // Use direct addressing if possible for more efficient delivery
      // Find the relevant peers based on role
      const targetRole = role === 'sender' ? 'receiver' : 'sender';
      const targetSockets = [];
      
      for (const [id, data] of participants.entries()) {
        if (id !== socket.id && data.role === targetRole && data.connected) {
          const targetSocket = io.sockets.sockets.get(id);
          if (targetSocket) {
            targetSockets.push(targetSocket);
          }
        }
      }
      
      // Create a compact progress update with only necessary fields
      const progressData = {
        progress,
        role
      };
      
      // Add optional fields only if they exist
      if (timestamp) progressData.timestamp = timestamp;
      if (speedFormatted) progressData.speedFormatted = speedFormatted;
      
      // Send directly to relevant peers if found
      if (targetSockets.length > 0) {
        targetSockets.forEach(targetSocket => {
          targetSocket.emit('progress-update', progressData);
        });
      } else {
        // Fall back to room broadcast if no specific targets found
        socket.to(roomId).emit('progress-update', progressData);
      }
    } catch (err) {
      console.error(`Error handling progress-update for ${socket.id} in room ${roomId}:`, err);
    }
  });

  // Handle 'announce-presence' event for reconnection detection
  socket.on('announce-presence', ({ roomId, role, peerId }) => {
    try {
      if (!roomId || !socket.rooms.has(roomId)) return;
      
      console.log(`👋 User ${socket.id} announced presence in room ${roomId}`);
      
      // Update the participant with latest data
      const participants = getRoomParticipants(roomId);
      if (participants.has(socket.id)) {
        const currentData = participants.get(socket.id);
        participants.set(socket.id, {
          ...currentData,
          role: role || currentData.role,
          peerId: peerId || currentData.peerId,
          connected: true,
          lastSeen: Date.now()
        });
      }
      
      // Force an update of the room participants for everyone
      io.to(roomId).emit('room-participants', {
        participants: Array.from(participants.entries()).map(([id, data]) => ({
          id,
          peerId: data.peerId,
          ...data
        }))
      });
      
      // Send additional reconnection notification for immediate UI update
      socket.to(roomId).emit('peer-reconnected', {
        socketId: socket.id,
        peerId: peerId,
        role: role
      });
    } catch (err) {
      console.error(`Error handling announce-presence for ${socket.id} in room ${roomId}:`, err);
    }
  });

  // Add handler for chunk acknowledgments from receiver to sender
  socket.on('chunk-received', ({ roomId, chunkIndex, success, error, sendId }) => {
    try {
      if (!roomId || !socket.rooms.has(roomId)) return;
      
      // Only log errors or milestone chunks to reduce console spam
      if (!success || error || chunkIndex % 100 === 0 || chunkIndex < 5) {
        console.log(`Chunk ${chunkIndex} acknowledgment from ${socket.id} in room ${roomId}: ${success ? 'success' : 'failed'}`);
      }
      
      // Clean up the in-progress tracking for the chunk if it exists
      if (inProgressChunks.has(chunkIndex)) {
        inProgressChunks.delete(chunkIndex);
      }
      
      // Use direct addressing if possible to improve performance for busy rooms
      // Find the sender in the room
      const participants = getRoomParticipants(roomId);
      let senderId = null;
      
      // Look for the sender
      for (const [id, data] of participants.entries()) {
        if (data.role === 'sender' && data.connected) {
          senderId = id;
          break;
        }
      }
      
      // If we found a direct sender, send directly to them; otherwise broadcast to room
      const ackData = {
        chunkIndex,
        success,
        error,
        receiverId: socket.id,
        sendId  // Make sure to forward the sendId if it exists
      };
      
      if (senderId) {
        // Direct send to the identified sender socket
        const senderSocket = io.sockets.sockets.get(senderId);
        if (senderSocket) {
          senderSocket.emit('chunk-received', ackData);
          
          // If sendId is provided, also send direct acknowledgment
          if (sendId) {
            senderSocket.emit(`chunk-received-${sendId}`, success ? 'success' : 'error');
          }
        } else {
          // Fallback to room broadcast if sender socket not found
          socket.to(roomId).emit('chunk-received', ackData);
        }
      } else {
        // Broadcast to room if sender not identified
        socket.to(roomId).emit('chunk-received', ackData);
      }
      
      // If there's an error, log it for debugging
      if (!success && error) {
        console.error(`Chunk ${chunkIndex} error: ${error}`);
      }
      
      // Update transfer state if it exists
      const state = transferStates.get(roomId);
      if (state) {
        state.lastUpdated = Date.now();
        if (success) {
          // Track the highest successful chunk by this receiver
          if (!state.receiverChunks) {
            state.receiverChunks = new Map();
          }
          
          const currentHighest = state.receiverChunks.get(socket.id) || -1;
          if (chunkIndex > currentHighest) {
            state.receiverChunks.set(socket.id, chunkIndex);
          }
          
          // Update the overall last acked chunk value
          if (chunkIndex > (state.lastAckedChunk || -1)) {
            state.lastAckedChunk = chunkIndex;
          }
        }
      }
    } catch (err) {
      console.error(`Error handling chunk-received for ${socket.id} in room ${roomId}:`, err);
    }
  });
  
  // Handle chunk acknowledgments (the modern implementation)
  socket.on('chunk-ack', ({ roomId, chunkIndex, sendId, receivedAt }) => {
    try {
      if (!roomId || !socket.rooms.has(roomId)) return;
      
      // Only log milestone chunks to reduce console spam
      if (chunkIndex % 100 === 0 || chunkIndex < 5) {
        console.log(`✅ User ${socket.id} acknowledged chunk ${chunkIndex} in room ${roomId}`);
      }
      
      // Find the sender in the room
      const participants = getRoomParticipants(roomId);
      let senderId = null;
      
      // Look for the sender
      for (const [id, data] of participants.entries()) {
        if (data.role === 'sender' && data.connected) {
          senderId = id;
          break;
        }
      }
      
      if (senderId) {
        // Direct send to the sender using the specific event formats expected by client
        const senderSocket = io.sockets.sockets.get(senderId);
        if (senderSocket) {
          // Send both the specific sendId acknowledgment and the room-level ack
          // These match the event names the client is listening for
          if (sendId) {
            senderSocket.emit(`chunk-ack-${sendId}`, { 
              chunkIndex, 
              receiverId: socket.id, 
              receivedAt 
            });
          }
          
          // Also send room-level ack as fallback
          senderSocket.emit(`chunk-ack-room-${roomId}-${chunkIndex}`, { 
            chunkIndex, 
            receiverId: socket.id, 
            receivedAt 
          });
        } else {
          // Fallback to room broadcast if sender socket not found
          socket.to(roomId).emit(`chunk-ack-room-${roomId}-${chunkIndex}`, { 
            chunkIndex, 
            receiverId: socket.id, 
            receivedAt 
          });
        }
      } else {
        // Broadcast to room if sender not identified
        socket.to(roomId).emit(`chunk-ack-room-${roomId}-${chunkIndex}`, { 
          chunkIndex, 
          receiverId: socket.id, 
          receivedAt 
        });
      }
      
      // Update transfer state if it exists
      const state = transferStates.get(roomId);
      if (state) {
        state.lastUpdated = Date.now();
        
        // Track the highest acknowledged chunk by this receiver
        if (!state.receiverChunks) {
          state.receiverChunks = new Map();
        }
        
        const currentHighest = state.receiverChunks.get(socket.id) || -1;
        if (chunkIndex > currentHighest) {
          state.receiverChunks.set(socket.id, chunkIndex);
        }
        
        // Update the overall last acked chunk value
        if (chunkIndex > (state.lastAckedChunk || -1)) {
          state.lastAckedChunk = chunkIndex;
        }
      }
    } catch (err) {
      console.error(`Error handling chunk-ack for ${socket.id} in room ${roomId}:`, err);
    }
  });
  
  // Add handler for explicit chunk requests (for retries)
  socket.on('request-chunk', ({ roomId, chunkIndex }) => {
    try {
      if (!roomId || !socket.rooms.has(roomId)) return;
      
      console.log(`User ${socket.id} requesting chunk ${chunkIndex} in room ${roomId}`);
      
      // Relay the request to the sender
      socket.to(roomId).emit('request-chunk', {
        chunkIndex,
        requesterId: socket.id
      });
    } catch (err) {
      console.error(`Error handling request-chunk for ${socket.id} in room ${roomId}:`, err);
    }
  });

  // Handle request for immediate room update
  socket.on('request-room-update', ({ roomId }) => {
    try {
      if (!roomId || !socket.rooms.has(roomId)) return;
      
      console.log(`🔄 User ${socket.id} requested room update for ${roomId}`);
      
      // Get the current participants
      const participants = getRoomParticipants(roomId);
      
      // Send an immediate update to all room participants
      io.to(roomId).emit('room-participants', {
        participants: Array.from(participants.entries()).map(([id, data]) => ({
          id,
          peerId: data.peerId,
          ...data
        }))
      });
    } catch (err) {
      console.error(`Error handling room update request for ${socket.id} in room ${roomId}:`, err);
    }
  });

  // Handle sender readiness to resume file transfer
  socket.on('sender-ready-to-resume', ({ roomId, fileName, fileSize, fileType }) => {
    try {
      if (!roomId || !socket.rooms.has(roomId)) return;
      
      console.log(`🔄 Sender ${socket.id} is ready to resume file transfer in room ${roomId}`);
      
      // Update the transfer state if it exists
      const state = transferStates.get(roomId);
      if (state) {
        state.senderId = socket.id;
        state.senderDisconnected = false;
        state.lastUpdated = Date.now();
        
        // Include additional file information
        if (!state.metadata) {
          state.metadata = { fileName, fileSize, fileType };
        }
      } else {
        // Create a new transfer state entry
        transferStates.set(roomId, {
          metadata: { fileName, fileSize, fileType },
          lastUpdated: Date.now(),
          senderId: socket.id,
          senderDisconnected: false
        });
      }
      
      // Update participant status
      const participants = getRoomParticipants(roomId);
      if (participants.has(socket.id)) {
        const participant = participants.get(socket.id);
        participant.role = 'sender';
        participant.lastSeen = Date.now();
        participant.connected = true;
        participant.fileName = fileName;
        participant.fileSize = fileSize;
      }
      
      // Forward to all receivers in the room
      socket.to(roomId).emit('sender-ready-to-resume', { 
        fileName, 
        fileSize, 
        fileType,
        senderId: socket.id
      });
    } catch (err) {
      console.error(`Error handling sender ready to resume in room ${roomId}:`, err);
    }
  });
  
  // Handle sender file selected event
  socket.on('sender-file-selected', ({ roomId, fileName, fileSize }) => {
    try {
      if (!roomId || !socket.rooms.has(roomId)) return;
      
      console.log(`📂 Sender ${socket.id} selected file ${fileName} in room ${roomId}`);
      
      // Update transfer state
      let state = transferStates.get(roomId);
      if (!state) {
        state = {
          lastUpdated: Date.now(),
          senderId: socket.id
        };
        transferStates.set(roomId, state);
      }
      
      state.senderId = socket.id;
      state.fileName = fileName;
      state.fileSize = fileSize;
      state.lastUpdated = Date.now();
      
      // Update participant info
      const participants = getRoomParticipants(roomId);
      if (participants.has(socket.id)) {
        const participant = participants.get(socket.id);
        participant.role = 'sender';
        participant.status = 'ready';
        participant.fileName = fileName;
        participant.fileSize = fileSize;
        participant.lastSeen = Date.now();
        participant.connected = true;
      }
      
      // Forward to all receivers
      socket.to(roomId).emit('sender-file-selected', {
        fileName,
        fileSize,
        senderId: socket.id
      });
    } catch (err) {
      console.error(`Error handling sender file selected in room ${roomId}:`, err);
    }
  });

  // Add a new event to handle room expiration
  socket.on('expire-previous-room', (roomId) => {
    try {
      if (!roomId) return;
      
      console.log(`🔒 Marking room ${roomId} as expired by user ${socket.id}`);
      
      // Mark the room as expired
      expiredRooms.set(roomId, Date.now());
      
      // Notify everyone in the room that it's expired
      io.to(roomId).emit('room-expired', { roomId });
      
      // Force everyone to leave the room
      const socketsInRoom = io.sockets.adapter.rooms.get(roomId);
      if (socketsInRoom) {
        for (const socketId of socketsInRoom) {
          const clientSocket = io.sockets.sockets.get(socketId);
          if (clientSocket && clientSocket.id !== socket.id) {
            clientSocket.leave(roomId);
            console.log(`Forced socket ${socketId} to leave expired room ${roomId}`);
          }
        }
      }
      
      // Update room status in our tracking
      activeRooms.delete(roomId);
      roomParticipants.delete(roomId);
      transferStates.delete(roomId);
      
      // Socket that requested expiration will handle its own reconnection to new room
    } catch (err) {
      console.error(`Error expiring room ${roomId}:`, err);
    }
  });
});

// Health check endpoint
app.get('/health', (req, res) => {
  res.status(200).json({
    status: 'ok',
    connections: io.engine.clientsCount,
    activeRooms: Array.from(activeRooms),
    activeTransfers: Array.from(transferStates.keys())
  });
});

// সার্ভার রান করানো
const PORT = process.env.PORT || 5000;
server.listen(PORT, () => {
  console.log(`🚀 Server running on http://localhost:${PORT}`);
});
