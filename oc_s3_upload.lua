local upload = require "resty.upload"
local s3_multi_upload = require "resty.s3_multi_upload"
local cjson = require "cjson"
local awss3 = require "resty.s3"
local util = require "resty.s3_util"
local httpc = require("resty.http").new()

local filename
for item in string.gmatch(ngx.var.uri, "[^/]+") do
    filename = item
end

local req_reader = httpc:get_client_body_reader()

s3 = awss3:new("07852C1HPZQ7FRJ6S5C5", "egoXz8nHe31YVRau5ZPAJyiRKaPpvDWykhKDpbyU", "test", {timeout=1000*30, host="oss-dev.ge.cn", ssl=true, ssl_verify=true})

local headers = ngx.req.get_headers()
local new_headers = {}
new_headers["Content-Length"]=headers["Content-Length"]
new_headers["content-type"] = headers["content-type"]
new_headers["x-amz-content-sha256"]=headers["x-amz-content-sha256"]
new_headers["x-amz-date"]=headers["x-amz-date"]
new_headers["x-amz-storage-class"]=headers["x-amz-storage-class"]
new_headers["Host"]="oss-dev.ge.cn"

if ngx.req.get_method() == "POST" and ngx.req.get_uri_args()['uploads'] == true then
    local short_uri = s3:get_short_uri(filename)
    local url = "https://" .. s3.host .. short_uri .. "?uploads"

    local authorization = s3.auth:authorization_v4("POST", url, new_headers, nil)
    ngx.log(ngx.INFO, "headers [", cjson.encode(myheaders), "]")

    -- TODO: check authorization.
    local res, err, req_debug = util.http_post(url, "", new_headers, s3.timeout)
    if not res then
        ngx.log(ngx.ERR, "fail request to aws s3 service: [", req_debug, "] err: ", err)
        return false, "request to aws s3 failed", 500
    end

    ngx.log(ngx.INFO, "aws s3 request:", url, ", status:", res.status, ",body:", tostring(res.body))

    if res.status ~= 200 then
        ngx.log(ngx.ERR, "request [ ", req_debug,  " ] failed! status:", res.status, ", body:", tostring(res.body))
        return false, res.body or "request to aws s3 failed", res.status
    end

    ngx.log(ngx.INFO, "aws returned: body:", res.body)
    ngx.header["Content-Type"] = "application/xml"
    ngx.say(res.body)
else
    if ngx.req.get_uri_args()["partNumber"] ~= nil and ngx.req.get_uri_args()["uploadId"] ~= nil then
        local uploadDetail = {}
        uploadDetail["Bucket"] = s3.aws_bucket
        uploadDetail["Key"] = filename
        uploadDetail["UploadId"] = ngx.req.get_uri_args()["uploadId"]
        local upload = s3_multi_upload:new(s3.auth, s3.host, s3.timeout,s3.ssl, s3.ssl_verify, uploadDetail)
        local ok, res = upload:upload(ngx.req.get_uri_args()["partNumber"],req_reader, new_headers)
        if not ok then
            ngx.say("upload [" .. filename .. "] part to s3 failed! ")
        end
        ngx.header["Etag"] = res.headers["Etag"]
    else
        if ngx.req.get_uri_args()["uploadId"] ~= nil then
            local uploadDetail = {}
            uploadDetail["Bucket"] = s3.aws_bucket
            uploadDetail["Key"] = filename
            uploadDetail["UploadId"] = ngx.req.get_uri_args()["uploadId"]
            local upload = s3_multi_upload:new(s3.auth, s3.host, s3.timeout,s3.ssl, s3.ssl_verify, uploadDetail)
            local ok, res = upload:complete(new_headers,req_reader)
            if not ok then
                ngx.say("upload [" .. filename .. "] complete to s3 failed!" )
            end
            ngx.header["Etag"] = res.headers["Etag"]
        else
            local ok, res = s3:put(filename, req_reader, new_headers)
            if not ok then
                ngx.say("upload [" .. filename .. "] direct to s3 failed!")
            end
            ngx.header["Etag"] = res.headers["Etag"]
        end
    end
    
end

