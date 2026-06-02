"use strict";

const fs = require("fs");
const http = require("http");
const os = require("os");
const path = require("path");
const { spawn } = require("child_process");

const ROOT = path.resolve(__dirname, "../../..");
const NODE_RED = path.join(ROOT, "node_modules", ".bin", "node-red");

function getFreePort() {
  return new Promise((resolve, reject) => {
    const server = http.createServer();
    server.listen(0, "127.0.0.1", () => {
      const port = server.address().port;
      server.close(() => resolve(port));
    });
    server.on("error", reject);
  });
}

function waitForHttp(url, timeoutMs = 30000) {
  const deadline = Date.now() + timeoutMs;
  return new Promise((resolve, reject) => {
    function attempt() {
      const req = http.get(url, (res) => {
        res.resume();
        resolve();
      });
      req.on("error", (err) => {
        if (Date.now() > deadline) {
          reject(err);
        } else {
          setTimeout(attempt, 250);
        }
      });
      req.setTimeout(1000, () => {
        req.destroy();
      });
    }
    attempt();
  });
}

function baseFlow(options) {
  return [
    {
      id: "flow1",
      type: "tab",
      label: "Playwright",
      disabled: false,
      info: "",
      env: [],
    },
    {
      id: "redis-config-1",
      type: "redis-config",
      name: "Playwright Redis",
      options: JSON.stringify(options, null, 2),
      cluster: false,
      optionsType: "json",
    },
    {
      id: "inject-ping",
      type: "inject",
      z: "flow1",
      name: "ping",
      props: [],
      repeat: "",
      crontab: "",
      once: false,
      onceDelay: 0.1,
      topic: "",
      wires: [["redis-ping"]],
      x: 150,
      y: 100,
    },
    {
      id: "redis-ping",
      type: "redis-command",
      z: "flow1",
      server: "redis-config-1",
      command: "PING",
      name: "PING",
      topic: "",
      params: "[]",
      paramsType: "json",
      block: false,
      wires: [["debug-pong"]],
      x: 350,
      y: 100,
    },
    {
      id: "debug-pong",
      type: "debug",
      z: "flow1",
      name: "pong",
      active: true,
      tosidebar: true,
      console: false,
      tostatus: false,
      complete: "payload",
      targetType: "msg",
      statusVal: "",
      statusType: "auto",
      wires: [],
      x: 560,
      y: 100,
    },
  ];
}

function writeSettings(userDir) {
  const settingsPath = path.join(userDir, "settings.js");
  const settings = `
"use strict";

module.exports = {
  flowFile: "flows.json",
  userDir: ${JSON.stringify(userDir)},
  nodesDir: [${JSON.stringify(ROOT)}],
  credentialSecret: "playwright-secret",
  editorTheme: {
    tours: false,
    projects: { enabled: false },
    palette: { editable: false }
  },
  logging: {
    console: {
      level: "warn",
      metrics: false,
      audit: false
    }
  }
};
`;
  fs.writeFileSync(settingsPath, settings);
  return settingsPath;
}

async function startNodeRed(initialOptions) {
  const userDir = fs.mkdtempSync(path.join(os.tmpdir(), "node-red-contrib-redis-pw-"));
  fs.mkdirSync(path.join(userDir, "lib", "functions"), { recursive: true });
  fs.writeFileSync(path.join(userDir, "flows.json"), JSON.stringify(baseFlow(initialOptions), null, 2));
  fs.writeFileSync(path.join(userDir, "flows_cred.json"), "{}");
  const settingsPath = writeSettings(userDir);
  const port = await getFreePort();
  const child = spawn(
    NODE_RED,
    ["--settings", settingsPath, "--userDir", userDir, "--port", String(port), "--no-telemetry"],
    {
      cwd: ROOT,
      env: process.env,
      stdio: ["ignore", "pipe", "pipe"],
    }
  );
  const output = [];
  child.stdout.on("data", (chunk) => output.push(chunk.toString()));
  child.stderr.on("data", (chunk) => output.push(chunk.toString()));

  let stopped = false;
  const stop = async () => {
    if (stopped) {
      return;
    }
    stopped = true;
    await new Promise((resolve) => {
      child.once("exit", resolve);
      child.kill("SIGINT");
      setTimeout(() => child.kill("SIGTERM"), 3000);
      setTimeout(() => child.kill("SIGKILL"), 6000);
    });
    fs.rmSync(userDir, { recursive: true, force: true });
  };

  try {
    await waitForHttp(`http://127.0.0.1:${port}/`);
  } catch (err) {
    await stop();
    throw new Error(`Node-RED did not start: ${err.message}\n${output.join("")}`);
  }

  return {
    url: `http://127.0.0.1:${port}/`,
    userDir,
    output,
    stop,
  };
}

async function openEditor(page, url) {
  await page.goto(url);
  await page.waitForFunction(() => window.RED && RED.nodes && RED.editor && RED.comms);
  await page.waitForFunction(() => RED.nodes.node("redis-ping") && RED.nodes.node("redis-config-1"));
  await page.waitForSelector("#red-ui-header-button-deploy", { state: "visible" });
}

async function openRedisConfig(page) {
  await page.evaluate(() => {
    RED.editor.editConfig("", "redis-config", "redis-config-1");
  });
  await page.waitForSelector("#node-config-dialog-edit-form", { state: "visible" });
  await page.waitForSelector("#node-config-input-name", { state: "visible" });
}

async function saveConfigDialog(page) {
  await page.locator("#node-config-dialog-ok").click();
  await page.locator("#node-config-dialog-edit-form").waitFor({ state: "hidden" });
}

async function deploy(page) {
  await page.evaluate(() => {
    RED.nodes.dirty(true);
  });
  await page.locator("#red-ui-header-button-deploy").click();
  await page.waitForFunction(() => !RED.nodes.dirty());
}

async function clearDebug(page) {
  await page.evaluate(() => {
    if (window.RED && RED.debug) {
      RED.debug.clearMessageList(true);
    }
  });
}

async function injectPing(page) {
  const response = await page.request.post(new URL("inject/inject-ping", page.url()).toString());
  if (!response.ok()) {
    throw new Error(`Inject request failed with HTTP ${response.status()}`);
  }
}

async function expectDebugPong(page) {
  await page.waitForFunction(() => {
    return Array.from(document.querySelectorAll(".red-ui-debug-msg .red-ui-debug-msg-payload")).some((el) =>
      /PONG/.test(el.textContent || "")
    );
  }, null, { timeout: 10000 });
}

async function setSelectValue(page, selector, value) {
  await page.locator(selector).selectOption(value);
  await page.locator(selector).dispatchEvent("change");
}

async function setInputValue(page, selector, value) {
  const input = page.locator(selector);
  await input.fill(value);
  await input.dispatchEvent("change");
}

async function setCodeEditorValue(page, editorId, value) {
  await page.evaluate(
    ({ editorId, value }) => {
      const stack = RED.editor.getEditStack ? RED.editor.getEditStack() : [];
      const activeEditor = stack.length > 0 ? stack[stack.length - 1].editor : null;
      const editor =
        activeEditor && typeof activeEditor.setValue === "function"
          ? activeEditor
          : typeof ace !== "undefined"
            ? ace.edit(editorId)
            : null;
      if (!editor || typeof editor.setValue !== "function") {
        throw new Error(`No code editor found for ${editorId}`);
      }
      editor.setValue(value, -1);
      if (typeof editor.clearSelection === "function") {
        editor.clearSelection();
      }
    },
    { editorId, value }
  );
}

async function runPingCheck(page) {
  await clearDebug(page);
  await injectPing(page);
  await expectDebugPong(page);
}

module.exports = {
  openEditor,
  openRedisConfig,
  saveConfigDialog,
  setCodeEditorValue,
  setInputValue,
  setSelectValue,
  startNodeRed,
  deploy,
  runPingCheck,
};
