// IndexedDB ব্যবহার করে চাংক সেভ করার জন্য

import { openDB } from 'idb';

// IndexedDB ডাটাবেজ খুলছি
export const initDB = async () => {
  return await openDB('p2p-file-share', 1, {
    upgrade(db) {
      if (!db.objectStoreNames.contains('chunks')) {
        db.createObjectStore('chunks', { keyPath: 'id', autoIncrement: true });
      }
    }
  });
};

// চাংক সেভ করা
export const addChunk = async (chunk) => {
  const db = await initDB();
  await db.add('chunks', { chunk });
};

// সব চাংক পড়া
export const getAllChunks = async () => {
  const db = await initDB();
  return await db.getAll('chunks');
};

// সব চাংক ডিলিট করা
export const clearChunks = async () => {
  const db = await initDB();
  await db.clear('chunks');
};
