"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (g && (g = 0, op[0] && (_ = 0)), _) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
Object.defineProperty(exports, "__esModule", { value: true });
var pg_1 = require("pg");
var node_crypto_1 = require("node:crypto");
var node_worker_threads_1 = require("node:worker_threads");
var messages_1 = require("../messages");
var DEFAULT_WORKER_POOL_SIZE = 4;
var parseConfig = function (configData) {
    var parsedJson;
    try {
        parsedJson = JSON.parse(configData);
    }
    catch (e) {
        return null;
    }
    if (!(parsedJson !== null &&
        "queues" in parsedJson &&
        "executorPaths" in parsedJson &&
        "postgresConn" in parsedJson)) {
        // Config is invalid
        return null;
    }
    return parsedJson;
};
var startWorker = function (config) {
    var worker = new node_worker_threads_1.Worker(new URL("./job-worker.js", import.meta.url));
    return worker;
};
var init = function (configData) { return __awaiter(void 0, void 0, void 0, function () {
    var config, postgresClient, queues, _i, _a, _b, name_1, queueConfig, workerPool, i, id, worker;
    return __generator(this, function (_c) {
        switch (_c.label) {
            case 0:
                config = parseConfig(configData);
                if (config === null) {
                    throw new Error("Invalid Worker setup data");
                }
                postgresClient = new pg_1.Client(config.postgresConn);
                return [4 /*yield*/, postgresClient.connect()];
            case 1:
                _c.sent();
                queues = new Map();
                for (_i = 0, _a = Object.entries(config.queues); _i < _a.length; _i++) {
                    _b = _a[_i], name_1 = _b[0], queueConfig = _b[1];
                    queues.set(name_1, {
                        name: name_1,
                        activeJobs: [],
                        maxConcurrency: queueConfig.maxConcurrency,
                    });
                }
                workerPool = [];
                for (i = 0; i < DEFAULT_WORKER_POOL_SIZE; i++) {
                    id = (0, node_crypto_1.randomUUID)();
                    worker = startWorker(config);
                    if (!worker) {
                        // TODO: Log why worker was not created
                        continue;
                    }
                    workerPool.push({
                        id: id,
                        state: "starting",
                        worker: worker,
                    });
                }
                return [2 /*return*/, {
                        queues: queues,
                        workerPool: workerPool,
                        postgresClient: postgresClient,
                    }];
        }
    });
}); };
var jobFromSchema = function (dbJob) {
    return {
        id: String(dbJob.id),
        worker: dbJob.worker,
        queue: dbJob.queue,
        args: JSON.parse(dbJob.args),
        insertedAt: dbJob.inserted_at,
        completedAt: dbJob.completed_at,
        state: dbJob.state === "available" ||
            dbJob.state === "completed" ||
            dbJob.state === "cancelled"
            ? dbJob.state
            : "UNKNOWN",
        attempts: dbJob.attempts,
        maxAttempts: dbJob.max_attempts,
    };
};
var createRepo = function (postgresClient) {
    var getFirstAvailableJobForQueue = function (queueName) { return __awaiter(void 0, void 0, void 0, function () {
        var result;
        var _a;
        return __generator(this, function (_b) {
            switch (_b.label) {
                case 0: return [4 /*yield*/, postgresClient.query("SELECT\n\t\t\t\t\t*\n\t\t\t\tFROM\n\t\t\t\t\tjobs\n\t\t\t\tWHERE\n\t\t\t\t\t(state = 'available'\n\t\t\t\t\t\tOR state = 'retryable')\n\t\t\t\t\tAND queue = $1::text\n\t\t\t\t\tAND scheduled_at <= NOW()\n\t\t\t\tLIMIT 1;", [queueName])];
                case 1:
                    result = _b.sent();
                    if (!result)
                        return [2 /*return*/, null];
                    return [2 /*return*/, {
                            rowCount: (_a = result.rowCount) !== null && _a !== void 0 ? _a : 0,
                            rows: result.rows.map(jobFromSchema),
                        }];
            }
        });
    }); };
    var markJobExecuting = function (jobId) { return __awaiter(void 0, void 0, void 0, function () {
        return __generator(this, function (_a) {
            return [2 /*return*/, postgresClient.query("UPDATE jobs SET state = 'executing', attempts = attempts +1 WHERE id = $1::int4;", [jobId])];
        });
    }); };
    return {
        getFirstAvailableJobForQueue: getFirstAvailableJobForQueue,
        markJobExecuting: markJobExecuting
    };
};
var filterReadyWorkers = function (p) {
    return p.filter(function (worker) {
        return worker.state === "ready";
    });
};
var mainLoop = function (queues, workerPool, postgresClient) { return __awaiter(void 0, void 0, void 0, function () {
    var lastUsedWorkerIndex, repo, readyWorkers, shouldSleep, _i, _a, _b, queueName, queue, jobsForQueue, job;
    return __generator(this, function (_c) {
        switch (_c.label) {
            case 0:
                lastUsedWorkerIndex = -1;
                repo = createRepo(postgresClient);
                _c.label = 1;
            case 1:
                if (!true) return [3 /*break*/, 11];
                readyWorkers = filterReadyWorkers(workerPool);
                if (!(readyWorkers.length === 0)) return [3 /*break*/, 3];
                return [4 /*yield*/, new Promise(function (resolve) {
                        setTimeout(resolve, 3000);
                    })];
            case 2:
                _c.sent();
                return [3 /*break*/, 1];
            case 3:
                shouldSleep = true;
                _i = 0, _a = queues.entries();
                _c.label = 4;
            case 4:
                if (!(_i < _a.length)) return [3 /*break*/, 8];
                _b = _a[_i], queueName = _b[0], queue = _b[1];
                return [4 /*yield*/, repo.getFirstAvailableJobForQueue(queueName)];
            case 5:
                jobsForQueue = _c.sent();
                if (!jobsForQueue || jobsForQueue.rowCount === 0) {
                    return [3 /*break*/, 7];
                }
                job = jobsForQueue.rows[0];
                if (queue.activeJobs.length >= queue.maxConcurrency) {
                    console.log("reached concurrency limits");
                    return [3 /*break*/, 7];
                }
                return [4 /*yield*/, assignJobToWorker(queue, job, repo, readyWorkers, lastUsedWorkerIndex)];
            case 6:
                lastUsedWorkerIndex = _c.sent();
                shouldSleep = false;
                _c.label = 7;
            case 7:
                _i++;
                return [3 /*break*/, 4];
            case 8:
                if (!shouldSleep) return [3 /*break*/, 10];
                return [4 /*yield*/, new Promise(function (resolve) {
                        setTimeout(resolve, 1000);
                    })];
            case 9:
                _c.sent();
                _c.label = 10;
            case 10: return [3 /*break*/, 1];
            case 11: return [2 /*return*/];
        }
    });
}); };
var roundRobinSelectWorker = function (p, lastUsedIndex) {
    var nextIndex = (lastUsedIndex + 1) % p.length;
    var worker = p[nextIndex].worker;
    return [worker, nextIndex];
};
var addActiveJobToQueue = function (queue, id) {
    queue.activeJobs.push({
        id: id,
        startedAt: new Date(),
    });
};
var assignJobToWorker = function (queue, job, repo, workerPool, lastUsedWorkerIndex) { return __awaiter(void 0, void 0, void 0, function () {
    var _a, worker, newIndex;
    return __generator(this, function (_b) {
        switch (_b.label) {
            case 0:
                _a = roundRobinSelectWorker(workerPool, lastUsedWorkerIndex), worker = _a[0], newIndex = _a[1];
                addActiveJobToQueue(queue, job.id);
                return [4 /*yield*/, repo.markJobExecuting(job.id)];
            case 1:
                _b.sent();
                worker.postMessage({
                    label: "execute",
                    job: job
                });
                // From here, the Worker updates execution state.
                return [2 /*return*/, newIndex];
        }
    });
}); };
var handleMessage = function (event) { return __awaiter(void 0, void 0, void 0, function () {
    var _a, _b, queues, workerPool, postgresClient;
    return __generator(this, function (_c) {
        switch (_c.label) {
            case 0:
                _a = event.data.label;
                switch (_a) {
                    case "init": return [3 /*break*/, 1];
                }
                return [3 /*break*/, 3];
            case 1: return [4 /*yield*/, init(event.data.config)];
            case 2:
                _b = _c.sent(), queues = _b.queues, workerPool = _b.workerPool, postgresClient = _b.postgresClient;
                mainLoop(queues, workerPool, postgresClient);
                return [3 /*break*/, 4];
            case 3:
                console.log("Unknown event kind: ".concat(JSON.stringify(event)));
                _c.label = 4;
            case 4: return [2 /*return*/];
        }
    });
}); };
self.onmessage = (0, messages_1.requireLabel)(handleMessage);
