do
require("rhttp")
assert(rhttp ~= nil and rhttp_stats ~= nil and rhttp_cancel ~= nil, "[rhttp-test] module not loaded")

local BASE_URL = "https://httpbin.org" -- test
local TIMEOUT = 15
local WATCHDOG_DELAY = 45

local function banner()
    print("[rhttp-test] ================= rhttp self-test =================")
end

local T = { cases = {}, results = {}, started = {}, reported = false, t0 = 0 }

local function log(text)
    print("[rhttp-test] " .. text)
end

local function summary()
    if T.reported then return end
    local passed, failed = 0, 0
    for _, c in ipairs(T.cases) do
        local r = T.results[c.name]
        if r == true then
            passed = passed + 1
        elseif r == false then
            failed = failed + 1
        else
            return
        end
    end
    T.reported = true
    log(string.format("done: %d passed, %d failed (%dms)",
        passed, failed, math.floor((os.clock() - T.t0) * 1000)))
    PrintTable(rhttp_stats())
end

local function elapsed(name)
    local s = T.started[name]
    if s == nil then return "" end
    return string.format(" (%dms)", math.floor((os.clock() - s) * 1000))
end

local function finish(name, ok, extra)
    if T.results[name] ~= nil then return end
    T.results[name] = ok and true or false
    if ok then
        log("PASS " .. name .. elapsed(name))
    else
        log("FAIL " .. name .. elapsed(name)
            .. (extra ~= nil and " (" .. tostring(extra) .. ")" or ""))
    end
    summary()
end

local function verify(name, fn, ...)
    local ok, result = pcall(fn, ...)
    if not ok then
        finish(name, false, result)
    elseif result == false then
        finish(name, false)
    else
        finish(name, true)
    end
end

local function expect(name, opts, fn)
    opts.success = function(status, body, headers)
        verify(name, fn, status, body, headers)
    end
    opts.failed = function(reason)
        finish(name, false, reason)
    end
    if not rhttp(opts) then
        finish(name, false, "not queued")
    end
end

local function expect_failed(name, opts, fn)
    opts.success = function()
        finish(name, false, "unexpected success")
    end
    opts.failed = function(reason)
        verify(name, fn, reason)
    end
    if not rhttp(opts) then
        finish(name, false, "not queued")
    end
end

local function expect_sync_fail(name, opts, pattern)
    local reason
    opts.failed = function(r) reason = r end
    local queued = rhttp(opts)
    finish(name, queued == false and reason ~= nil
        and (pattern == nil or string.find(reason, pattern, 1, true) ~= nil), reason)
end

local function case(name, run)
    T.cases[#T.cases + 1] = { name = name, run = run }
end

case("get", function()
    expect("get", { url = BASE_URL .. "/get", timeout = TIMEOUT },
        function(status, body, headers)
            return status == 200
                and string.find(body, "httpbin", 1, true) ~= nil
                and headers["content-type"] ~= nil
        end)
end)

case("query params", function()
    expect("query params", {
        url = BASE_URL .. "/get",
        parameters = { a = "1", b = "hello world" },
        timeout = TIMEOUT,
    }, function(status, body)
        return status == 200
            and string.find(body, '"a": "1"', 1, true) ~= nil
            and string.find(body, "hello world", 1, true) ~= nil
    end)
end)

case("post json", function()
    expect("post json", {
        url = BASE_URL .. "/post",
        method = "POST",
        headers = { ["Content-Type"] = "application/json" },
        body = util.TableToJSON({ event = "test", n = 42 }),
        timeout = TIMEOUT,
    }, function(status, body)
        return status == 200
            and string.find(body, '"event": "test"', 1, true) ~= nil
    end)
end)

case("post form", function()
    expect("post form", {
        url = BASE_URL .. "/post",
        method = "POST",
        parameters = { username = "player", password = "secret" },
        timeout = TIMEOUT,
    }, function(status, body)
        return status == 200
            and string.find(body, '"username": "player"', 1, true) ~= nil
    end)
end)

case("headers", function()
    expect("headers", {
        url = BASE_URL .. "/headers",
        headers = { ["X-Test-Header"] = "hello123" },
        timeout = TIMEOUT,
    }, function(status, body)
        return status == 200
            and string.find(body, "hello123", 1, true) ~= nil
            and string.find(body, "gmsv-rhttp", 1, true) ~= nil
    end)
end)

case("404", function()
    expect("404", { url = BASE_URL .. "/status/404", timeout = TIMEOUT },
        function(status)
            return status == 404
        end)
end)

case("timeout", function()
    local started = os.time()
    expect_failed("timeout", { url = BASE_URL .. "/delay/8", timeout = 2 },
        function()
            return os.time() - started < 10
        end)
end)

case("cancel", function()
    local ok, id = rhttp({
        url = BASE_URL .. "/delay/10",
        timeout = 30,
        success = function()
            finish("cancel", false, "success after cancel")
        end,
        failed = function(reason)
            verify("cancel", function(r)
                return r == "Request cancelled"
            end, reason)
        end,
    })
    if not ok then
        finish("cancel", false, "not queued")
    elseif rhttp_cancel(id) ~= true then
        finish("cancel", false, "cancel rejected")
    end
end)

case("no url", function()
    expect_sync_fail("no url", {})
end)

case("bad scheme", function()
    expect_sync_fail("bad scheme", { url = "ftp://example.com/x" }, "http")
end)

case("managed header", function()
    expect_sync_fail("managed header", {
        url = BASE_URL .. "/get",
        headers = { ["Content-Length"] = "5" },
    }, "managed")
end)

case("credentials in url", function()
    expect_sync_fail("credentials in url", { url = "https://user:pass@example.com/" }, "credentials")
end)

case("stats", function()
    local stats = rhttp_stats()
    finish("stats", stats.submitted ~= nil and stats.in_flight ~= nil, "no stats table")
end)

T.t0 = os.clock()
for _, c in ipairs(T.cases) do
    T.started[c.name] = os.clock()
    local ok, err = pcall(c.run)
    if not ok then finish(c.name, false, err) end
end

timer.Simple(WATCHDOG_DELAY, function()
    for _, c in ipairs(T.cases) do
        if T.results[c.name] == nil then
            finish(c.name, false, "no callback in " .. WATCHDOG_DELAY .. "s")
        end
    end
end)

banner()
log("running " .. #T.cases .. " cases")

end