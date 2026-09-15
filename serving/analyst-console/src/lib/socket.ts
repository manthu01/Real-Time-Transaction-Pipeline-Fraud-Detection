'use client';

import { io, type Socket } from 'socket.io-client';
import { API_BASE_URL } from './api';

let socket: Socket | null = null;

/** One shared socket for the whole console (a module-level singleton, not
 * a hook-scoped connection per component) so opening the drill-down page
 * in a second tab doesn't open a second connection per mounted component
 * -- callers just add/remove their own event listeners on the shared
 * instance. */
export function getSocket(): Socket {
  if (!socket) {
    socket = io(API_BASE_URL, { transports: ['websocket'] });
  }
  return socket;
}
