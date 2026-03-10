// bench_node.js — MySQL benchmark using mysql2/promise
// npm install mysql2
// node bench_node.js

import mysql from "mysql2/promise";

// ─── Config ──────────────────────────────────────────────────────────
const DB = {
    host:     "127.0.0.1",
    port:     3307,
    user:     "testuser",
    password: "testpass",
    database: "shop",
};

const CONCURRENT = 50;
const ITERATIONS = 200;

// ─── Helpers ─────────────────────────────────────────────────────────

async function makeConn() {
    return mysql.createConnection(DB);
}

async function bench(label, fn) {
    // warmup
    await fn();

    const start = performance.now();
    await fn();
    const elapsed = performance.now() - start;

    const qps = Math.round(ITERATIONS / (elapsed / 1000));
    console.log(
        label.padEnd(35) + " | " +
        (elapsed.toFixed(2) + " ms").padEnd(12) +
        ` | ${qps} q/s`
    );
}

// ─── Scenarios ───────────────────────────────────────────────────────

// 1. Sequential SELECT
async function sequentialSelect() {
    const conn = await makeConn();
    for (let i = 1; i <= ITERATIONS; i++) {
        await conn.execute("SELECT * FROM users WHERE id = ?", [i % 10 + 1]);
    }
    await conn.end();
}

// 2. Concurrent SELECT — N parallel promises
async function concurrentSelect() {
    const perFiber = Math.ceil(ITERATIONS / CONCURRENT);
    const tasks = Array.from({ length: CONCURRENT }, async (_, f) => {
        const conn = await makeConn();
        for (let i = 0; i < perFiber; i++) {
            await conn.execute("SELECT * FROM users WHERE id = ?", [(f + i) % 10 + 1]);
        }
        await conn.end();
    });
    await Promise.all(tasks);
}

// 3. Sequential INSERT
async function sequentialInsert() {
    const conn = await makeConn();
    for (let i = 0; i < ITERATIONS; i++) {
        await conn.execute("INSERT INTO users (name) VALUES (?)", [`BenchUser_${i}`]);
    }
    await conn.end();
}

// 4. Concurrent INSERT
async function concurrentInsert() {
    const perFiber = Math.ceil(ITERATIONS / CONCURRENT);
    const tasks = Array.from({ length: CONCURRENT }, async (_, f) => {
        const conn = await makeConn();
        for (let i = 0; i < perFiber; i++) {
            await conn.execute("INSERT INTO users (name) VALUES (?)", [`Fiber${f}_Row${i}`]);
        }
        await conn.end();
    });
    await Promise.all(tasks);
}

// 5. Mixed R+W
async function mixedRW() {
    const perFiber = Math.ceil(ITERATIONS / CONCURRENT);
    const tasks = Array.from({ length: CONCURRENT }, async (_, f) => {
        const conn = await makeConn();
        for (let i = 0; i < perFiber; i++) {
            if (i % 2 === 0) {
                await conn.execute("SELECT * FROM users WHERE id = ?", [i % 10 + 1]);
            } else {
                await conn.execute("INSERT INTO users (name) VALUES (?)", [`Mixed${f}_${i}`]);
            }
        }
        await conn.end();
    });
    await Promise.all(tasks);
}

// ─── Main ─────────────────────────────────────────────────────────────

const SEP = "─".repeat(62);

console.log(`\nNode.js mysql2/promise — MySQL Benchmark`);
console.log(`Concurrent tasks  : ${CONCURRENT}`);
console.log(`Total iterations  : ${ITERATIONS}`);
console.log(SEP);
console.log("Scenario".padEnd(35) + " | " + "Time".padEnd(12) + " | Throughput");
console.log(SEP);

await bench(`Sequential SELECT (×${ITERATIONS})`,          sequentialSelect);
await bench(`Concurrent SELECT (×${CONCURRENT} tasks)`,    concurrentSelect);
await bench(`Sequential INSERT (×${ITERATIONS})`,          sequentialInsert);
await bench(`Concurrent INSERT (×${CONCURRENT} tasks)`,    concurrentInsert);
await bench(`Mixed R+W (×${CONCURRENT} tasks)`,            mixedRW);

console.log(SEP);
console.log("Done.\n");
