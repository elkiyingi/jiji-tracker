self.addEventListener('install', (e) => {
  self.skipWaiting();
});

self.addEventListener('activate', (e) => {
  return self.clients.claim();
});

// A simple pass-through fetch handler for PWA installability requirements
self.addEventListener('fetch', (e) => {
  e.respondWith(fetch(e.request).catch(() => new Response("Network error.")));
});
