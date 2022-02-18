local upload = require "resty.upload"
local cjson = require "cjson"
local awss3 = require "resty.s3"
local util = require "resty.s3_util"

local function body_iterator(pre,form)
    return function()
        if pre ~= nil then
            local data = pre
            pre = nil
            ngx.say("body: ", data)
            return data
        end
        typ, res, err = form:read()
        if typ == "body" then
            ngx.say("body type: ", type(res))
            if not typ then
                ngx.say("failed to read: ", err)
                return nil
            end
    
            ngx.say("body: ", res)
            return res
        end
        return nil
    end
end


s3 = awss3:new("07852C1HPZQ7FRJ6S5C5", "egoXz8nHe31YVRau5ZPAJyiRKaPpvDWykhKDpbyU", "test", {timeout=1000*30, host="oss-dev.ge.cn", ssl=true, ssl_verify=true})

local headers = ngx.req.get_headers()
local content_length = headers['content-length']
ngx.say("headers: ", cjson.encode(headers))

local chunk_size = 4096 -- should be set to 4096 or 8192
                        -- for real-world settings

local form, err = upload:new(chunk_size)
if not form then
    ngx.log(ngx.ERR, "failed to new upload: ", err)
    ngx.exit(500)
end

form:set_timeout(1000) -- 1 sec

local typ, res, err = form:read()
while typ == "header" do
    if not typ then
        ngx.say("failed to read: ", err)
        return
    end

    if not filename then
        for k, v in pairs(res) do
            filename = string.match(v,"filename=\"(.+)\"")
            if filename ~= nil then
                break
            end
        end
    end

    ngx.say("header: ", cjson.encode(res))
    typ, res, err = form:read()
end
ngx.say("filename:",filename)

if typ == "body" then
    local headers = util.new_headers()
    headers['Content-Length'] = content_length
    local ok, resp = s3:put(filename, body_iterator(res,form), headers)
    if not ok then
        ngx.say("upload [" .. filename .. "] to s3 failed! resp:" .. tostring(resp))
    end
end
-- while typ == "body" do
--     ngx.say('body type: ', type(res))
--     if not typ then
--         ngx.say("failed to read: ", err)
--         return
--     end

--     ngx.say("body: ", cjson.encode(res))
--     typ, res, err = form:read()
-- end
-- if typ == "part_end" then
--     break
-- end

-- typ, res, err = form:read()
-- ngx.say("read: ", cjson.encode({typ, res}))