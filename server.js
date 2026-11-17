
"use strict";

const express = require("express");
const path = require("path");
const fs = require("fs");
const crypto = require("crypto");
const admin = require("firebase-admin");

const app = express();
const PORT = process.env.PORT || 10000;

const API_TOKEN = process.env.FOOTBALL_DATA_TOKEN;
const API_BASE = "https://api.football-data.org/v4";
const TIME_ZONE = "Africa/Lagos";
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD;

app.disable("x-powered-by");
app.use(express.json({ limit: "100kb" }));
app.use(express.static(__dirname));

/* =====================================
   FIREBASE ADMIN CONNECTION
===================================== */

let firebaseReady = false;
let db = null;
let messaging = null;

try {
  const serviceAccountPath =
    "/etc/secrets/firebase-service-account.json";

  const serviceAccount = JSON.parse(
    fs.readFileSync(serviceAccountPath, "utf8")
  );

  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });

  db = admin.firestore();
  messaging = admin.messaging();

  firebaseReady = true;

  console.log("Firebase notification system connected.");

} catch (error) {
  console.error(
    "Firebase initialization failed:",
    error.message
  );
}

/* =====================================
   ADMIN AUTHENTICATION
===================================== */

const adminSessions = new Map();

const ADMIN_SESSION_DURATION =
  8 * 60 * 60 * 1000;

function safeCompare(first, second) {
  const a = Buffer.from(String(first || ""));
  const b = Buffer.from(String(second || ""));

  if (a.length !== b.length) return false;

  return crypto.timingSafeEqual(a, b);
}

function createAdminSession() {
  const token = crypto.randomBytes(48).toString("hex");

  adminSessions.set(token, {
    expiresAt: Date.now() + ADMIN_SESSION_DURATION
  });

  return token;
}

function requireAdmin(req, res, next) {
  const authorization = req.headers.authorization || "";

  const token = authorization.startsWith("Bearer ")
    ? authorization.slice(7)
    : "";

  const session = adminSessions.get(token);

  if (!session || Date.now() > session.expiresAt) {
    if (token) adminSessions.delete(token);

    return res.status(401).json({
      error: "Administrator authentication required."
    });
  }

  next();
}

/* =====================================
   NOTIFICATION HELPERS
===================================== */

function hashToken(token) {
  return crypto
    .createHash("sha256")
    .update(token)
    .digest("hex");
}

function notificationErrorIsInvalidToken(error) {
  return [
    "messaging/registration-token-not-registered",
    "messaging/invalid-registration-token"
  ].includes(error?.code);
}

function cleanNotificationText(value, maxLength) {
  return String(value || "")
    .trim()
    .slice(0, maxLength);
}

async function getSubscriberCount() {
  const snapshot = await db
    .collection("notificationSubscribers")
    .get();

  return snapshot.size;
}

/* =====================================
   CACHE STORAGE
===================================== */

const fixtureCache = {
  data: null,
  expiresAt: 0,
  dateFrom: null,
  dateTo: null
};

const analysisCache = new Map();
const teamCache = new Map();
const matchCache = new Map();
const h2hCache = new Map();

const teamPending = new Map();
const h2hPending = new Map();
const analysisPending = new Map();

let pendingFixtures = null;

/* =====================================
   API REQUEST QUEUE
===================================== */

const API_REQUEST_INTERVAL = 6500;

const apiQueue = [];
const apiPendingRequests = new Map();

let apiQueueTimer = null;
let apiRequestRunning = false;
let lastApiRequestAt = 0;
let queueSequence = 0;

function priorityWeight(priority) {
  if (priority === "high") return 0;
  if (priority === "normal") return 1;
  return 2;
}

function normalizePriority(priority) {
  if (priority === "high") return "high";
  if (priority === "normal") return "normal";
  return "background";
}

function promoteFootballRequest(endpoint, priority) {
  const task = apiPendingRequests.get(endpoint);

  if (!task || task.state !== "queued") return;

  if (
    priorityWeight(priority) <
    priorityWeight(task.priority)
  ) {
    task.priority = priority;

    if (apiQueueTimer) {
      clearTimeout(apiQueueTimer);
      apiQueueTimer = null;
    }

    processApiQueue();
  }
}

function processApiQueue() {
  if (
    apiRequestRunning ||
    apiQueueTimer ||
    !apiQueue.length
  ) {
    return;
  }

  const wait = Math.max(
    0,
    API_REQUEST_INTERVAL - (Date.now() - lastApiRequestAt)
  );

  apiQueueTimer = setTimeout(async () => {
    apiQueueTimer = null;

    if (apiRequestRunning || !apiQueue.length) {
      processApiQueue();
      return;
    }

    apiQueue.sort((a, b) => {
      const difference =
        priorityWeight(a.priority) -
        priorityWeight(b.priority);

      return difference || a.sequence - b.sequence;
    });

    const task = apiQueue.shift();

    if (!task) return;

    task.state = "running";
    apiRequestRunning = true;
    lastApiRequestAt = Date.now();

    try {
      console.log(
        "Football API:",
        task.endpoint,
        "| Priority:",
        task.priority
      );

      const response = await fetch(
        API_BASE + task.endpoint,
        {
          method: "GET",
          headers: {
            "X-Auth-Token": API_TOKEN,
            "Accept": "application/json"
          },
          signal: AbortSignal.timeout(25000)
        }
      );

      if (!response.ok) {
        const body = await response.text();

        const error = new Error(
          `Football API HTTP ${response.status}: ${body}`
        );

        error.status = response.status;
        throw error;
      }

      task.resolve(await response.json());

    } catch (error) {
      task.reject(error);

    } finally {
      if (apiPendingRequests.get(task.endpoint) === task) {
        apiPendingRequests.delete(task.endpoint);
      }

      apiRequestRunning = false;
      processApiQueue();
    }

  }, wait);
}

function footballRequest(endpoint, priority = "background") {
  priority = normalizePriority(priority);

  if (!API_TOKEN) {
    return Promise.reject(
      new Error("FOOTBALL_DATA_TOKEN is not configured.")
    );
  }

  const existing = apiPendingRequests.get(endpoint);

  if (existing) {
    promoteFootballRequest(endpoint, priority);
    return existing.promise;
  }

  let resolveRequest;
  let rejectRequest;

  const promise = new Promise((resolve, reject) => {
    resolveRequest = resolve;
    rejectRequest = reject;
  });

  const task = {
    endpoint,
    priority,
    state: "queued",
    sequence: queueSequence++,
    promise,
    resolve: resolveRequest,
    reject: rejectRequest
  };

  apiPendingRequests.set(endpoint, task);
  apiQueue.push(task);

  processApiQueue();

  return promise;
}

/* =====================================
   NIGERIA DATE RANGE
===================================== */

function getNigeriaDate() {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: TIME_ZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit"
  }).formatToParts(new Date());

  const values = {};

  for (const part of parts) {
    values[part.type] = part.value;
  }

  return `${values.year}-${values.month}-${values.day}`;
}

function addDays(dateString, days) {
  const date = new Date(`${dateString}T12:00:00Z`);

  date.setUTCDate(date.getUTCDate() + days);

  return date.toISOString().slice(0, 10);
}

function getFixtureDateRange() {
  const dateFrom = getNigeriaDate();

  return {
    dateFrom,
    dateTo: addDays(dateFrom, 7)
  };
}

/* =====================================
   CACHE HELPERS
===================================== */

function cacheGet(cache, key) {
  const item = cache.get(key);

  if (!item || Date.now() > item.expiresAt) {
    cache.delete(key);
    return null;
  }

  return item.data;
}

function cacheSet(cache, key, data, minutes) {
  cache.set(key, {
    data,
    expiresAt: Date.now() + minutes * 60 * 1000
  });
}

/* =====================================
   MATCH INFORMATION
===================================== */

function saveFixtureMatches(matches) {
  for (const match of matches || []) {
    if (!match?.id) continue;

    cacheSet(
      matchCache,
      String(match.id),
      match,
      60
    );
  }
}

async function getMatchInformation(matchId, priorityState) {
  const key = String(matchId);

  const cached = cacheGet(matchCache, key);

  if (cached) return cached;

  const match = await footballRequest(
    `/matches/${encodeURIComponent(key)}`,
    priorityState.priority
  );

  cacheSet(matchCache, key, match, 60);

  return match;
}

/* =====================================
   POISSON MODEL
===================================== */

function poisson(lambda, goals) {
  if (!Number.isFinite(lambda) || lambda < 0) {
    return 0;
  }

  let probability = Math.exp(-lambda);

  for (let i = 1; i <= goals; i++) {
    probability *= lambda / i;
  }

  return probability;
}

function calculateProbabilities(homeXG, awayXG) {
  let home = 0;
  let draw = 0;
  let away = 0;

  let over05 = 0;
  let over15 = 0;
  let over25 = 0;
  let over35 = 0;

  let bttsYes = 0;

  const maxGoals = 12;

  for (let h = 0; h <= maxGoals; h++) {
    for (let a = 0; a <= maxGoals; a++) {
      const probability =
        poisson(homeXG, h) *
        poisson(awayXG, a);

      if (h > a) home += probability;
      else if (h === a) draw += probability;
      else away += probability;

      const total = h + a;

      if (total >= 1) over05 += probability;
      if (total >= 2) over15 += probability;
      if (total >= 3) over25 += probability;
      if (total >= 4) over35 += probability;

      if (h > 0 && a > 0) {
        bttsYes += probability;
      }
    }
  }

  return {
    homeWin: home * 100,
    draw: draw * 100,
    awayWin: away * 100,

    doubleChance1X: (home + draw) * 100,
    doubleChanceX2: (draw + away) * 100,
    doubleChance12: (home + away) * 100,

    over05: over05 * 100,
    under05: (1 - over05) * 100,

    over15: over15 * 100,
    under15: (1 - over15) * 100,

    over25: over25 * 100,
    under25: (1 - over25) * 100,

    over35: over35 * 100,
    under35: (1 - over35) * 100,

    bttsYes: bttsYes * 100,
    bttsNo: (1 - bttsYes) * 100
  };
}

/* =====================================
   TEAM STATISTICS
===================================== */

function getTeamStats(matches, teamId) {
  const finished = (matches || [])
    .filter(match =>
      match.status === "FINISHED" &&
      match.score?.fullTime?.home != null &&
      match.score?.fullTime?.away != null
    )
    .sort((a, b) =>
      new Date(b.utcDate) - new Date(a.utcDate)
    );

  const recent = finished.slice(0, 10);

  const homeMatches = finished
    .filter(match => match.homeTeam?.id === teamId)
    .slice(0, 10);

  const awayMatches = finished
    .filter(match => match.awayTeam?.id === teamId)
    .slice(0, 10);

  function calculateGroup(group) {
    let played = 0;
    let wins = 0;
    let draws = 0;
    let losses = 0;
    let goalsFor = 0;
    let goalsAgainst = 0;

    const form = [];

    for (const match of group) {
      const isHome = match.homeTeam?.id === teamId;

      const scored = isHome
        ? match.score.fullTime.home
        : match.score.fullTime.away;

      const conceded = isHome
        ? match.score.fullTime.away
        : match.score.fullTime.home;

      played++;
      goalsFor += scored;
      goalsAgainst += conceded;

      if (scored > conceded) {
        wins++;
        form.push("W");
      } else if (scored === conceded) {
        draws++;
        form.push("D");
      } else {
        losses++;
        form.push("L");
      }
    }

    return {
      played,
      wins,
      draws,
      losses,
      goalsFor,
      goalsAgainst,
      averageGoalsFor: played ? goalsFor / played : 0,
      averageGoalsAgainst: played ? goalsAgainst / played : 0,
      form
    };
  }

  const overall = calculateGroup(recent);
  const home = calculateGroup(homeMatches);
  const away = calculateGroup(awayMatches);

  return {
    ...overall,
    home,
    away
  };
}

/* =====================================
   TEAM HISTORY
===================================== */

async function getTeamHistory(teamId, priorityState) {
  const key = String(teamId);

  const cached = cacheGet(teamCache, key);

  if (cached) return cached;

  const existing = teamPending.get(key);

  if (existing) {
    promoteFootballRequest(
      existing.endpoint,
      priorityState.priority
    );

    return existing.promise;
  }

  const endpoint =
    `/teams/${encodeURIComponent(key)}/matches?status=FINISHED&limit=30`;

  const request = footballRequest(
    endpoint,
    priorityState.priority
  )
    .then(data => {
      const matches = (data.matches || [])
        .filter(match =>
          match.status === "FINISHED" &&
          match.score?.fullTime?.home != null &&
          match.score?.fullTime?.away != null
        )
        .sort((a, b) =>
          new Date(b.utcDate) - new Date(a.utcDate)
        );

      cacheSet(teamCache, key, matches, 60);

      return matches;
    })
    .finally(() => {
      teamPending.delete(key);
    });

  teamPending.set(key, {
    endpoint,
    promise: request
  });

  return request;
}

/* =====================================
   HEAD-TO-HEAD HISTORY
===================================== */

async function getHeadToHead(matchId, priorityState) {
  const key = String(matchId);

  const cached = cacheGet(h2hCache, key);

  if (cached) return cached;

  const existing = h2hPending.get(key);

  if (existing) {
    promoteFootballRequest(
      existing.endpoint,
      priorityState.priority
    );

    return existing.promise;
  }

  const endpoint =
    `/matches/${encodeURIComponent(key)}/head2head?limit=10`;

  const request = footballRequest(
    endpoint,
    priorityState.priority
  )
    .then(data => {
      const matches = (data.matches || [])
        .filter(match =>
          match.status === "FINISHED" &&
          match.score?.fullTime?.home != null &&
          match.score?.fullTime?.away != null
        )
        .sort((a, b) =>
          new Date(b.utcDate) - new Date(a.utcDate)
        );

      console.log(
        "H2H response:",
        matchId,
        "meetings:",
        matches.length
      );

      const result = { matches };

      cacheSet(h2hCache, key, result, 1440);

      return result;
    })
    .catch(error => {
      console.error(
        "H2H API request failed:",
        matchId,
        error.message
      );

      throw error;
    })
    .finally(() => {
      h2hPending.delete(key);
    });

  h2hPending.set(key, {
    endpoint,
    promise: request
  });

  return request;
}

/* =====================================
   CALCULATE HEAD-TO-HEAD STATISTICS
===================================== */

function calculateHeadToHead(matches, homeId, awayId) {
  const result = {
    available: false,
    played: 0,

    homeWins: 0,
    draws: 0,
    awayWins: 0,

    homeGoals: 0,
    awayGoals: 0,

    averageHomeGoals: 0,
    averageAwayGoals: 0,

    recentMeetings: []
  };

  for (const match of matches || []) {
    const matchHomeId = match.homeTeam?.id;
    const matchAwayId = match.awayTeam?.id;

    const homeGoals = match.score?.fullTime?.home;
    const awayGoals = match.score?.fullTime?.away;

    if (
      !(
        (matchHomeId === homeId && matchAwayId === awayId) ||
        (matchHomeId === awayId && matchAwayId === homeId)
      ) ||
      homeGoals == null ||
      awayGoals == null
    ) {
      continue;
    }

    let goalsFor;
    let goalsAgainst;

    if (matchHomeId === homeId) {
      goalsFor = homeGoals;
      goalsAgainst = awayGoals;
    } else {
      goalsFor = awayGoals;
      goalsAgainst = homeGoals;
    }

    result.played++;

    result.homeGoals += goalsFor;
    result.awayGoals += goalsAgainst;

    if (goalsFor > goalsAgainst) {
      result.homeWins++;
    } else if (goalsFor < goalsAgainst) {
      result.awayWins++;
    } else {
      result.draws++;
    }

    result.recentMeetings.push({
      date: match.utcDate,

      homeTeam: match.homeTeam?.name || "Unknown",
      awayTeam: match.awayTeam?.name || "Unknown",

      homeGoals,
      awayGoals,

      goalsFor,
      goalsAgainst,

      status: match.status
    });
  }

  if (result.played > 0) {
    result.available = true;

    result.averageHomeGoals =
      result.homeGoals / result.played;

    result.averageAwayGoals =
      result.awayGoals / result.played;
  }

  return result;
}

/* =====================================
   EXPECTED GOALS MODEL
===================================== */

function calculateExpectedGoals(homeStats, awayStats, headToHead) {
  const homeOverallAttack =
    homeStats.averageGoalsFor;

  const homeOverallDefence =
    homeStats.averageGoalsAgainst;

  const awayOverallAttack =
    awayStats.averageGoalsFor;

  const awayOverallDefence =
    awayStats.averageGoalsAgainst;

  const homeVenueAttack =
    homeStats.home.played
      ? homeStats.home.averageGoalsFor
      : homeOverallAttack;

  const homeVenueDefence =
    homeStats.home.played
      ? homeStats.home.averageGoalsAgainst
      : homeOverallDefence;

  const awayVenueAttack =
    awayStats.away.played
      ? awayStats.away.averageGoalsFor
      : awayOverallAttack;

  const awayVenueDefence =
    awayStats.away.played
      ? awayStats.away.averageGoalsAgainst
      : awayOverallDefence;

  const overallHomeXG =
    (homeOverallAttack + awayOverallDefence) / 2;

  const overallAwayXG =
    (awayOverallAttack + homeOverallDefence) / 2;

  const homeVenueWeight = Math.min(
    0.65,
    Math.min(homeStats.home.played, awayStats.away.played) / 10 * 0.65
  );

  const venueHomeXG =
    (homeVenueAttack + awayVenueDefence) / 2;

  const venueAwayXG =
    (awayVenueAttack + homeVenueDefence) / 2;

  let homeXG =
    overallHomeXG * (1 - homeVenueWeight) +
    venueHomeXG * homeVenueWeight;

  let awayXG =
    overallAwayXG * (1 - homeVenueWeight) +
    venueAwayXG * homeVenueWeight;

  let h2hWeight = 0;

  if (headToHead?.played > 0) {
    h2hWeight = Math.min(
      0.15,
      headToHead.played * 0.03
    );

    homeXG =
      homeXG * (1 - h2hWeight) +
      headToHead.averageHomeGoals * h2hWeight;

    awayXG =
      awayXG * (1 - h2hWeight) +
      headToHead.averageAwayGoals * h2hWeight;
  }

  homeXG = Math.max(0.2, Math.min(4, homeXG));
  awayXG = Math.max(0.2, Math.min(4, awayXG));

  return {
    homeXG,
    awayXG,
    venueWeight: homeVenueWeight,
    h2hWeight
  };
}

/* =====================================
   MARKET RECOMMENDATIONS
===================================== */

function makeRecommendations(probabilities) {
  const definitions = [
    ["Home Win", "homeWin"],
    ["Draw", "draw"],
    ["Away Win", "awayWin"],

    ["Double Chance 1X", "doubleChance1X"],
    ["Double Chance X2", "doubleChanceX2"],
    ["Double Chance 12", "doubleChance12"],

    ["Over 1.5 Goals", "over15"],
    ["Under 1.5 Goals", "under15"],

    ["Over 2.5 Goals", "over25"],
    ["Under 2.5 Goals", "under25"],

    ["Over 3.5 Goals", "over35"],
    ["Under 3.5 Goals", "under35"],

    ["BTTS Yes", "bttsYes"],
    ["BTTS No", "bttsNo"]
  ];

  return definitions
    .map(([market, key]) => ({
      market,
      key,
      probability: probabilities[key]
    }))
    .filter(item =>
      Number.isFinite(item.probability) &&
      item.probability > 0 &&
      item.probability <= 100
    )
    .sort((a, b) =>
      b.probability - a.probability
    )
    .map((item, index) => ({
      rank: index + 1,
      market: item.market,
      key: item.key,
      probability: Number(item.probability.toFixed(1)),

      confidence:
        item.probability >= 70
          ? "HIGH"
          : item.probability >= 55
            ? "MODERATE"
            : "LOW"
    }));
}

/* =====================================
   BUILD MATCH ANALYSIS
===================================== */

async function buildMatchAnalysis(matchId, priorityState) {
  const match = await getMatchInformation(
    matchId,
    priorityState
  );

  if (
    !match.homeTeam?.id ||
    !match.awayTeam?.id
  ) {
    const error = new Error(
      "Team information is unavailable."
    );

    error.status = 422;
    throw error;
  }

  const homeId = match.homeTeam.id;
  const awayId = match.awayTeam.id;

  const [homeMatches, awayMatches, h2hData] =
    await Promise.all([
      getTeamHistory(homeId, priorityState),
      getTeamHistory(awayId, priorityState),

      getHeadToHead(matchId, priorityState)
        .catch(error => {
          console.error(
            "H2H analysis unavailable:",
            error.message
          );

          return {
            matches: [],
            unavailable: true
          };
        })
    ]);

  const headToHead = calculateHeadToHead(
    h2hData.matches,
    homeId,
    awayId
  );

  const homeStats = getTeamStats(
    homeMatches,
    homeId
  );

  const awayStats = getTeamStats(
    awayMatches,
    awayId
  );

  if (!homeStats.played || !awayStats.played) {
    const error = new Error(
      "Not enough completed team matches for reliable analysis."
    );

    error.status = 422;
    error.homeMatchesAvailable = homeStats.played;
    error.awayMatchesAvailable = awayStats.played;

    throw error;
  }

  const expected = calculateExpectedGoals(
    homeStats,
    awayStats,
    headToHead
  );

  const homeXG = expected.homeXG;
  const awayXG = expected.awayXG;

  const probabilities = calculateProbabilities(
    homeXG,
    awayXG
  );

  const roundedProbabilities = Object.fromEntries(
    Object.entries(probabilities).map(([key, value]) => [
      key,
      Number(value.toFixed(1))
    ])
  );

  const recommendations = makeRecommendations(
    roundedProbabilities
  );

  /*
    VIP recommendation:
    No fixed 75% qualification threshold.
    The strongest supported market is returned.
  */

  const vipRecommendation =
    recommendations.length
      ? recommendations[0]
      : null;

  return {
    match: {
      id: match.id,
      utcDate: match.utcDate,
      status: match.status,

      homeTeam: {
        id: match.homeTeam.id,
        name: match.homeTeam.name,
        crest: match.homeTeam.crest || ""
      },

      awayTeam: {
        id: match.awayTeam.id,
        name: match.awayTeam.name,
        crest: match.awayTeam.crest || ""
      },

      competition: {
        id: match.competition?.id || null,
        name: match.competition?.name || "Unknown"
      },

      score: match.score || {}
    },

    model: {
      name: "GENERAL TIPS Statistical Model",

      expectedHomeGoals: Number(homeXG.toFixed(2)),
      expectedAwayGoals: Number(awayXG.toFixed(2)),

      matchesPerTeam: 10,

      homeVenueMatches: homeStats.home.played,
      awayVenueMatches: awayStats.away.played,

      h2hMatches: headToHead.played,

      h2hInfluence: Number(
        (expected.h2hWeight * 100).toFixed(1)
      ),

      venueInfluence: Number(
        (expected.venueWeight * 100).toFixed(1)
      ),

      note:
        "Estimates combine recent results, venue performance, head-to-head history and Poisson goal modelling."
    },

    teamForm: {
      home: homeStats,
      away: awayStats
    },

    headToHead,

    probabilities: roundedProbabilities,

    recommendations,

    vipRecommendation,

    limitations: [
      "Probabilities are statistical model estimates, not guarantees.",
      "Head-to-head influence is limited to reduce the effect of small historical samples.",
      "Recent results may include different competitions.",
      "Injuries, suspensions and confirmed line-ups are not included.",
      "The model does not use bookmaker odds.",
      "Venue statistics depend on available historical matches."
    ],

    generatedAt: new Date().toISOString()
  };
}

/* =====================================
   FIREBASE SUBSCRIPTION ROUTES
===================================== */

app.post("/api/notifications/subscribe", async (req, res) => {
  if (!firebaseReady) {
    return res.status(503).json({
      error: "Notification service is unavailable."
    });
  }

  const token = cleanNotificationText(
    req.body.token,
    4096
  );

  if (!token || token.length < 20) {
    return res.status(400).json({
      error: "A valid notification token is required."
    });
  }

  try {
    const id = hashToken(token);

    await db
      .collection("notificationSubscribers")
      .doc(id)
      .set({
        token,
        active: true,
        updatedAt: admin.firestore.FieldValue.serverTimestamp()
      }, { merge: true });

    res.json({
      ok: true,
      message: "Notification subscription saved."
    });

  } catch (error) {
    console.error("Subscription error:", error.message);

    res.status(500).json({
      error: "Unable to save notification subscription."
    });
  }
});

app.post("/api/notifications/unsubscribe", async (req, res) => {
  if (!firebaseReady) {
    return res.status(503).json({
      error: "Notification service is unavailable."
    });
  }

  const token = cleanNotificationText(
    req.body.token,
    4096
  );

  if (!token) {
    return res.status(400).json({
      error: "Notification token is required."
    });
  }

  try {
    await db
      .collection("notificationSubscribers")
      .doc(hashToken(token))
      .delete();

    res.json({
      ok: true,
      message: "Notification subscription removed."
    });

  } catch (error) {
    console.error("Unsubscribe error:", error.message);

    res.status(500).json({
      error: "Unable to remove subscription."
    });
  }
});

/* =====================================
   ADMIN LOGIN
===================================== */

app.post("/api/admin/login", (req, res) => {
  if (!ADMIN_PASSWORD) {
    return res.status(503).json({
      error: "ADMIN_PASSWORD is not configured."
    });
  }

  const password = req.body.password;

  if (!safeCompare(password, ADMIN_PASSWORD)) {
    return res.status(401).json({
      error: "Incorrect administrator password."
    });
  }

  const token = createAdminSession();

  res.json({
    ok: true,
    token,
    expiresIn: ADMIN_SESSION_DURATION
  });
});

/* =====================================
   ADMIN DASHBOARD DATA
===================================== */

app.get(
  "/api/admin/status",
  requireAdmin,
  async (req, res) => {
    if (!firebaseReady) {
      return res.status(503).json({
        error: "Firebase is unavailable."
      });
    }

    try {
      const subscriberSnapshot = await db
        .collection("notificationSubscribers")
        .get();

      const campaignsSnapshot = await db
        .collection("notificationCampaigns")
        .orderBy("createdAt", "desc")
        .limit(10)
        .get();

      const campaigns = campaignsSnapshot.docs.map(doc => ({
        id: doc.id,
        ...doc.data()
      }));

      res.json({
        ok: true,
        app: "GENERAL FOOTBALL TIPS V1.0",
        firebaseReady,
        subscribers: subscriberSnapshot.size,
        campaigns
      });

    } catch (error) {
      console.error("Admin status error:", error.message);

      res.status(500).json({
        error: "Unable to load administrator dashboard."
      });
    }
  }
);

/* =====================================
   ADMIN TEST NOTIFICATION
===================================== */

app.post(
  "/api/admin/test-notification",
  requireAdmin,
  async (req, res) => {
    if (!firebaseReady) {
      return res.status(503).json({
        error: "Firebase is unavailable."
      });
    }

    const token = cleanNotificationText(
      req.body.token,
      4096
    );

    const title = cleanNotificationText(
      req.body.title || "GENERAL FOOTBALL TIPS",
      100
    );

    const body = cleanNotificationText(
      req.body.body || "Your test notification is working.",
      300
    );

    if (!token) {
      return res.status(400).json({
        error: "Your device notification token is required."
      });
    }

    try {
      const messageId = await messaging.send({
        token,
        notification: {
          title,
          body
        },
        data: {
          type: "test",
          app: "GENERAL FOOTBALL TIPS"
        },
        webpush: {
          notification: {
            icon: "/icon-192.png",
            badge: "/icon-192.png"
          }
        }
      });

      res.json({
        ok: true,
        message: "Test notification sent.",
        messageId
      });

    } catch (error) {
      console.error("Test notification error:", error.message);

      res.status(400).json({
        error: "Unable to send test notification.",
        details: error.message
      });
    }
  }
);

/* =====================================
   MANUAL UPDATE APPROVAL
===================================== */

app.post(
  "/api/admin/approve-update",
  requireAdmin,
  async (req, res) => {
    if (!firebaseReady) {
      return res.status(503).json({
        error: "Firebase is unavailable."
      });
    }

    const version = cleanNotificationText(
      req.body.version,
      40
    );

    const title = cleanNotificationText(
      req.body.title,
      100
    );

    const body = cleanNotificationText(
      req.body.body,
      300
    );

    if (!version || !title || !body) {
      return res.status(400).json({
        error: "Version, title and announcement are required."
      });
    }

    const campaignId = crypto
      .createHash("sha256")
      .update(version.toLowerCase())
      .digest("hex")
      .slice(0, 40);

    const campaignRef = db
      .collection("notificationCampaigns")
      .doc(campaignId);

    try {
      const reservation = await db.runTransaction(async transaction => {
        const existing = await transaction.get(campaignRef);

        if (existing.exists) return false;

        transaction.create(campaignRef, {
          version,
          title,
          body,
          status: "sending",
          createdAt: admin.firestore.FieldValue.serverTimestamp(),
          sent: 0,
          failed: 0
        });

        return true;
      });

      if (!reservation) {
        return res.status(409).json({
          error: "This version has already been approved or submitted.",
          campaignId
        });
      }

      const subscribersSnapshot = await db
        .collection("notificationSubscribers")
        .get();

      const subscribers = subscribersSnapshot.docs;

      let sent = 0;
      let failed = 0;

      const invalidDocuments = [];

      for (let i = 0; i < subscribers.length; i += 500) {
        const batch = subscribers.slice(i, i + 500);

        const tokens = batch
          .map(doc => doc.data().token)
          .filter(Boolean);

        if (!tokens.length) continue;

        try {
          const result = await messaging.sendEachForMulticast({
            tokens,
            notification: {
              title,
              body
            },
            data: {
              type: "update",
              version,
              campaignId
            },
            webpush: {
              notification: {
                icon: "/icon-192.png",
                badge: "/icon-192.png"
              }
            }
          });

          sent += result.successCount;
          failed += result.failureCount;

          result.responses.forEach((response, index) => {
            if (
              !response.success &&
              notificationErrorIsInvalidToken(response.error)
            ) {
              invalidDocuments.push(batch[index].ref);
            }
          });

        } catch (error) {
          console.error(
            "Notification batch failed:",
            error.message
          );

          failed += tokens.length;
        }
      }

      for (const document of invalidDocuments) {
        try {
          await document.delete();
        } catch (error) {
          console.error(
            "Invalid subscription cleanup failed:",
            error.message
          );
        }
      }

      await campaignRef.update({
        status: failed === 0
          ? "completed"
          : "completed_with_errors",

        sent,
        failed,

        finishedAt:
          admin.firestore.FieldValue.serverTimestamp()
      });

      res.json({
        ok: true,
        message: "Manual update notification campaign completed.",
        version,
        sent,
        failed,
        campaignId
      });

    } catch (error) {
      console.error(
        "Update approval error:",
        error.message
      );

      try {
        await campaignRef.set({
          version,
          title,
          body,
          status: "failed",
          error: error.message,
          updatedAt:
            admin.firestore.FieldValue.serverTimestamp()
        }, { merge: true });

      } catch (recordError) {
        console.error(
          "Campaign record error:",
          recordError.message
        );
      }

      res.status(500).json({
        error: "The update notification campaign could not be completed.",
        details: error.message
      });
    }
  }
);

/* =====================================
   HEALTH CHECK
===================================== */

app.get("/api/health", (req, res) => {
  res.json({
    ok: true,
    app: "GENERAL FOOTBALL TIPS V1.0",

    apiKeyConfigured: Boolean(API_TOKEN),
    firebaseReady,
    adminPasswordConfigured: Boolean(ADMIN_PASSWORD),

    timezone: TIME_ZONE,

    queuedRequests: apiQueue.length,
    runningRequest: apiRequestRunning,

    cachedFixtures: Boolean(fixtureCache.data),
    cachedAnalyses: analysisCache.size,
    cachedTeams: teamCache.size,
    cachedMatches: matchCache.size,
    cachedH2H: h2hCache.size,

    pendingAnalyses: analysisPending.size
  });
});

/* =====================================
   FIXTURES
===================================== */

app.get("/api/fixtures", async (req, res) => {
  if (!API_TOKEN) {
    return res.status(503).json({
      error: "Football API token is not configured."
    });
  }

  const forceRefresh = req.query.refresh === "1";

  const { dateFrom, dateTo } = getFixtureDateRange();

  if (
    !forceRefresh &&
    fixtureCache.data &&
    Date.now() < fixtureCache.expiresAt &&
    fixtureCache.dateFrom === dateFrom &&
    fixtureCache.dateTo === dateTo
  ) {
    return res.json(fixtureCache.data);
  }

  if (!pendingFixtures) {
    pendingFixtures = footballRequest(
      `/matches?dateFrom=${dateFrom}&dateTo=${dateTo}`,
      "normal"
    )
      .then(data => {
        const matches = (data.matches || [])
          .sort((a, b) =>
            new Date(a.utcDate) - new Date(b.utcDate)
          );

        saveFixtureMatches(matches);

        const result = {
          count: matches.length,
          matches,
          dateFrom,
          dateTo,
          interval: "7 days",
          timezone: TIME_ZONE,
          updatedAt: new Date().toISOString()
        };

        fixtureCache.data = result;
        fixtureCache.dateFrom = dateFrom;
        fixtureCache.dateTo = dateTo;
        fixtureCache.expiresAt =
          Date.now() + 5 * 60 * 1000;

        return result;
      });
  }

  const request = pendingFixtures;

  try {
    const result = await request;
    res.json(result);

  } catch (error) {
    console.error("Fixture error:", error.message);

    res.status(error.status || 500).json({
      error: "Unable to load football fixtures.",
      details: error.message
    });

  } finally {
    if (pendingFixtures === request) {
      pendingFixtures = null;
    }
  }
});

/* =====================================
   MATCH ANALYSIS
===================================== */

app.get("/api/match/:id/analysis", async (req, res) => {
  const matchId = req.params.id;

  if (!/^\d+$/.test(matchId)) {
    return res.status(400).json({
      error: "Invalid match ID."
    });
  }

  const priority = normalizePriority(req.query.priority);

  const cached = cacheGet(analysisCache, matchId);

  if (cached) {
    return res.json(cached);
  }

  try {
    let pending = analysisPending.get(matchId);

    if (pending) {
      if (
        priorityWeight(priority) <
        priorityWeight(pending.priorityState.priority)
      ) {
        pending.priorityState.priority = priority;
      }

      promoteFootballRequest(
        `/matches/${matchId}`,
        priority
      );

    } else {
      const priorityState = { priority };

      const promise = buildMatchAnalysis(
        matchId,
        priorityState
      )
        .then(result => {
          cacheSet(analysisCache, matchId, result, 60);
          return result;
        })
        .finally(() => {
          analysisPending.delete(matchId);
        });

      pending = {
        promise,
        priorityState
      };

      analysisPending.set(matchId, pending);
    }

    const result = await pending.promise;

    res.json(result);

  } catch (error) {
    console.error(
      "Analysis error:",
      matchId,
      error.message
    );

    res.status(error.status || 500).json({
      error: "Unable to analyse this match.",
      details: error.message,
      homeMatchesAvailable: error.homeMatchesAvailable,
      awayMatchesAvailable: error.awayMatchesAvailable
    });
  }
});

/* =====================================
   FRONTEND ROUTE
===================================== */

app.get("*", (req, res) => {
  res.sendFile(
    path.join(__dirname, "index.html")
  );
});

/* =====================================
   SERVER START
===================================== */

app.listen(PORT, () => {
  console.log(
    `GENERAL FOOTBALL TIPS V1.0 running on port ${PORT}`
  );
});
