// app.js — shared Firebase init + auth helpers
// firebase-config.js is injected by deploy-ui.py before upload

const app  = firebase.initializeApp(FIREBASE_CONFIG);
const auth = firebase.auth();
const db   = firebase.firestore();

// ── Auth helpers ──────────────────────────────────────────────

function requireAuth(redirectTo = "index.html") {
  return new Promise((resolve) => {
    auth.onAuthStateChanged((user) => {
      if (!user) {
        window.location.href = redirectTo;
      } else {
        resolve(user);
      }
    });
  });
}

function redirectIfLoggedIn(to = "dashboard.html") {
  auth.onAuthStateChanged((user) => {
    if (user) window.location.href = to;
  });
}

async function getIdToken() {
  const user = auth.currentUser;
  if (!user) throw new Error("Not logged in");
  return user.getIdToken();
}

function formatTime(ts) {
  if (!ts) return "";
  const d = ts.toDate ? ts.toDate() : new Date(ts);
  return d.toLocaleString();
}

// ── API helpers ───────────────────────────────────────────────

async function apiPost(path, body) {
  const token = await getIdToken();
  const resp  = await fetch(API_URL + path, {
    method:  "POST",
    headers: {
      "Content-Type":  "application/json",
      "Authorization": "Bearer " + token,
    },
    body: JSON.stringify(body),
  });
  const data = await resp.json();
  if (!resp.ok) throw new Error(data.error || "API error " + resp.status);
  return data;
}

async function apiGet(path) {
  const token = await getIdToken();
  const resp  = await fetch(API_URL + path, {
    headers: { "Authorization": "Bearer " + token },
  });
  const data = await resp.json();
  if (!resp.ok) throw new Error(data.error || "API error " + resp.status);
  return data;
}

// ── Firestore helpers ─────────────────────────────────────────

function debatesRef(userId) {
  return db.collection("debates").where("userId", "==", userId).orderBy("startedAt", "desc");
}

async function saveDebate(userId, debateId, query) {
  await db.collection("debates").doc(debateId).set({
    userId:    userId,
    debateId:  debateId,
    query:     query,
    status:    "running",
    round:     1,
    maxRounds: 3,
    result:    null,
    startedAt: firebase.firestore.FieldValue.serverTimestamp(),
  });
}

async function updateDebate(debateId, fields) {
  await db.collection("debates").doc(debateId).update(fields);
}

async function getDebate(debateId) {
  const doc = await db.collection("debates").doc(debateId).get();
  return doc.exists ? { id: doc.id, ...doc.data() } : null;
}
