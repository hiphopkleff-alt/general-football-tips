"use strict";

/* =====================================
   FIREBASE CONFIGURATION
===================================== */

importScripts(
  "https://www.gstatic.com/firebasejs/13.3.0/firebase-app-compat.js"
);

importScripts(
  "https://www.gstatic.com/firebasejs/13.3.0/firebase-messaging-compat.js"
);

const firebaseConfig = {
  apiKey: "AIzaSyC29k6H32V4smIJmQbaJpmT8s-9gO-FJ9M",
  authDomain: "general-football-tips.firebaseapp.com",
  projectId: "general-football-tips",
  storageBucket: "general-football-tips.firebasestorage.app",
  messagingSenderId: "710183231768",
  appId: "1:710183231768:web:60c490825c687fb9c57e6d"
};

firebase.initializeApp(firebaseConfig);

const messaging = firebase.messaging();

/* =====================================
   BACKGROUND NOTIFICATIONS
===================================== */

messaging.onBackgroundMessage(payload => {
  console.log(
    "Firebase background notification:",
    payload
  );

  // Firebase automatically displays messages
  // that already contain a notification payload.

  if (payload.notification) {
    return;
  }

  // Display data-only messages manually.

  const title =
    payload.data?.title ||
    "GENERAL FOOTBALL TIPS";

  const options = {
    body:
      payload.data?.body ||
      "A new update is available.",

    icon: "/icon-192.png",
    badge: "/icon-192.png",

    data: {
      url: payload.data?.url || "/"
    }
  };

  return self.registration.showNotification(
    title,
    options
  );
});

/* =====================================
   NOTIFICATION CLICK
===================================== */

self.addEventListener("notificationclick", event => {
  event.notification.close();

  const targetUrl =
    event.notification.data?.url || "/";

  event.waitUntil(
    clients.matchAll({
      type: "window",
      includeUncontrolled: true
    }).then(windowClients => {

      for (const client of windowClients) {
        if ("focus" in client) {
          return client.focus().then(() =>
            client.navigate(targetUrl)
          );
        }
      }

      return clients.openWindow(targetUrl);
    })
  );
});

/* =====================================
   APP CACHE
===================================== */

const CACHE_NAME = "general-football-tips-v2";

const APP_FILES = [
  "/",
  "/index.html",
  "/manifest.json",
  "/icon-192.png",
  "/icon-512.png"
];

/* =====================================
   INSTALL
===================================== */

self.addEventListener("install", event => {
  event.waitUntil(
    caches.open(CACHE_NAME).then(cache =>
      cache.addAll(APP_FILES)
    )
  );

  self.skipWaiting();
});

/* =====================================
   ACTIVATE
===================================== */

self.addEventListener("activate", event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(
        keys
          .filter(key => key !== CACHE_NAME)
          .map(key => caches.delete(key))
      )
    )
  );

  self.clients.claim();
});

/* =====================================
   FETCH / OFFLINE CACHE
===================================== */

self.addEventListener("fetch", event => {
  const url = new URL(event.request.url);

  if (url.origin !== self.location.origin) return;

  if (url.pathname.startsWith("/api/")) return;

  if (event.request.method !== "GET") return;

  event.respondWith(
    fetch(event.request)
      .then(response => {
        if (response.ok) {
          const copy = response.clone();

          caches.open(CACHE_NAME).then(cache =>
            cache.put(event.request, copy)
          );
        }

        return response;
      })
      .catch(() =>
        caches.match(event.request).then(cached =>
          cached || caches.match("/")
        )
      )
  );
});
