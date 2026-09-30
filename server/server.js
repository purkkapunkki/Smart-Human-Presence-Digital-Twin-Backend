require("dotenv").config();
const http = require("http");
const express = require("express");
const cors = require("cors");
const { Server } = require("socket.io");

const state = require("./state");
const { validateSensorUpdate, validateLogin } = require("./schema");
const mapData = require("./map.json");

const PORT = process.env.PORT || 3000;
const BROADCAST_INTERVAL_MS = 500;
const CORS_ORIGIN = process.env.CORS_ORIGIN || "*";

const app = express();
app.use(cors({ origin: CORS_ORIGIN }));

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: CORS_ORIGIN },
});

app.get("/api/map", (req, res) => {
  res.json(mapData);
});

function broadcastState() {
  io.emit("state:update", state.getState());
}

io.on("connection", (socket) => {
  // Demo login flow (UC_1) — named users
  socket.on("login", (payload) => {
    if (!validateLogin(payload)) {
      socket.emit("login:error", { message: "Invalid login payload" });
      return;
    }
    if (state.isUsernameTaken(payload.username)) {
      socket.emit("login:error", { message: "Username already taken" });
      return;
    }
    const user = state.addLoggedInUser(socket.id, payload);
    socket.emit("login:success", user);
    broadcastState();
  });

  socket.on("user:move", (location) => {
    if (
      !location ||
      typeof location.x !== "number" ||
      typeof location.y !== "number"
    ) {
      return;
    }
    state.updateUserLocation(socket.id, location);
    broadcastState();
  });

  socket.on("logout", () => {
    state.removeLoggedInUser(socket.id);
    broadcastState();
  });

  // Sensor pipeline (Python client) — anonymous presence only
  socket.on("sensor:update", (payload) => {
    if (!validateSensorUpdate(payload)) {
      socket.emit("sensor:error", {
        message: "Invalid sensor event",
        errors: validateSensorUpdate.errors,
      });
      return;
    }
    state.upsertAnonymousPresence(payload);
  });

  socket.on("disconnect", () => {
    state.removeLoggedInUser(socket.id);
    broadcastState();
  });
});

// Periodic broadcast so anonymous presence updates reach clients even
// without a login/logout event triggering it.
setInterval(broadcastState, BROADCAST_INTERVAL_MS);

server.listen(PORT, () => {
  console.log(`Server listening on http://localhost:${PORT}`);
});
