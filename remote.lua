local cjson = require "cjson"
local httpc = require("resty.http").new()
local awss3 = require "resty.s3"
local util = require "resty.s3_util"

local HTTP_TIMEOUT = 1000*60*60
local SSL_VERIFY = true

local redirect_key = "/remote-redirected"
local path = string.sub(ngx.var.uri,12)

ngx.log(ngx.DEBUG, "Original Request Headers: ", cjson.encode(ngx.req.get_headers()))

if ngx.req.get_headers()["Content-Type"] == nil then
    ngx.req.set_header("Content-Type", "application/octet-stream")
end

local res
-- 上传前oc逻辑处理
if ngx.req.get_headers()["OC-CHUNKED"] then
    res = ngx.location.capture(
        redirect_key .. path,
        {
            method = ngx.HTTP_PUT,
            args = ngx.req.get_uri_args(),
        }
    )
else
    res = ngx.location.capture(
        redirect_key .. path,
        {
            method = ngx.HTTP_PUT,
            args = ngx.req.get_uri_args(),
            vars = { before_upload = "true", file_size = ngx.req.get_headers()["Content-Length"] },
            body = ""
        }
    )
end

ngx.log(ngx.DEBUG, "Before Upload Response", cjson.encode(res))
if res.status ~= ngx.HTTP_ACCEPTED  then
    ngx.status = res.status
    ngx.header = res.header
    return
end

local file_key = res.header["S3-File-Key"]
local endpoint = res.header["S3-Endpoint"]
local bucket = res.header["S3-Bucket"]
local region = res.header["S3-Region"]
local key = res.header["S3-Key"]
local secret = res.header["S3-Secret"]
local sig_version = res.header["S3-Signature-Version"]
local new_file = res.header["OC-New-File"]

-- 上传文件内容到s3
local schema, host, port, uri, args = util.full_url_parse(endpoint)
local ssl = true
if schema == "http" then
    ssl = false
end

local s3 = awss3:new(key, secret, bucket, {timeout=HTTP_TIMEOUT, host=host, ssl=ssl, ssl_verify=SSL_VERIFY, aws_region=region})

local s3_req_header = {}
s3_req_header["Content-Length"] = ngx.req.get_headers()["Content-Length"]
s3_req_header["Content-Type"] = ngx.req.get_headers()["Content-Type"]
s3_req_header["x-amz-storage-class"] = "STANDARD"

local req_reader = httpc:get_client_body_reader()

local ok, s3_res = s3:put(file_key, req_reader, s3_req_header)
if not ok then
    ngx.log(ngx.ERR, "upload [" .. file_key .. "] direct to s3 failed! Response: ", cjson.encode(s3_res))
    ngx.status = ngx.HTTP_INTERNAL_SERVER_ERROR
    return
end

ngx.log(debug, "S3 Response", cjson.encode(s3_res))

-- 上传后oc逻辑处理
res = ngx.location.capture(
        redirect_key .. path,
        {
            method = ngx.HTTP_PUT,
            args = ngx.req.get_uri_args(),
            vars = { after_upload = "true", new_file = new_file, file_key=file_key},
            body = ""
        }
    )
ngx.status = res.status
ngx.header = res.header