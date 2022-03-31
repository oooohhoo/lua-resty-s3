local cjson = require "cjson"
local httpc = require("resty.http").new()
local awss3 = require "resty.s3"
local util = require "resty.s3_util"
local s3_multi_upload = require "resty.s3_multi_upload"

local HTTP_TIMEOUT = 1000*60*60
local SSL_VERIFY = true
local AUTO_CHUNK_SIZE = 1024*1024*1024
local CHUNK_SIZE = 100*1024*1024
-- 注意：该值必须能够整除 CHUNK_SIZE，否则分块上传会出错
local BUFFER_SIZE = 65536

local redirect_key = "/remote-redirected"
local path = string.sub(ngx.var.uri,12)

if ngx.req.get_headers()["Content-Type"] == nil then
    ngx.req.set_header("Content-Type", "application/octet-stream")
end

function get_s3_header()
    local header = {}
    header["Content-Type"] = ngx.req.get_headers()["Content-Type"]
    header["x-amz-storage-class"] = "STANDARD"
    return header
end

local res
-- 上传前oc逻辑处理
res = ngx.location.capture(
    redirect_key .. path,
    {
        method = ngx.HTTP_PUT,
        args = ngx.req.get_uri_args(),
        vars = { before_upload = "true", file_size = ngx.req.get_headers()["Content-Length"] },
        body = ""
    }
)

if res.status ~= ngx.HTTP_ACCEPTED  then
    ngx.status = res.status
    ngx.header = res.header
    return
end

local s3_storage = res.header["S3-Storage"]
local file_key = res.header["S3-File-Key"]
local new_file = res.header["OC-New-File"]

if s3_storage == "0" then
    ngx.req.read_body()
    res = ngx.location.capture(
        redirect_key .. path,
        {
            method = ngx.HTTP_PUT,
            args = ngx.req.get_uri_args(),
            vars = { after_upload = "true", new_file = new_file, file_key=file_key}
        }
    )
    ngx.status = res.status
    ngx.header = res.header
    return
end


local endpoint = res.header["S3-Endpoint"]
local bucket = res.header["S3-Bucket"]
local region = res.header["S3-Region"]
local key = res.header["S3-Key"]
local secret = res.header["S3-Secret"]
local sig_version = res.header["S3-Signature-Version"]

-- 上传文件内容到s3
local schema, host, port, uri, args = util.full_url_parse(endpoint)
local ssl = true
if schema == "http" then
    ssl = false
end

if port and port ~= 80 and port ~= 443 then
   host = host .. ":" .. port
end

local s3 = awss3:new(key, secret, bucket, {timeout=HTTP_TIMEOUT, host=host, ssl=ssl, ssl_verify=SSL_VERIFY, aws_region=region})

local content_length = ngx.req.get_headers()["Content-Length"]

local req_reader = httpc:get_client_body_reader(BUFFER_SIZE)

if ngx.req.get_headers()["oc-chunked"] == "1" then
    local s3_req_header = get_s3_header()
    s3_req_header["Content-Length"] = content_length
    local upload_id = res.header["Upload-Id"]
    local uploadDetail = {}
    uploadDetail["Bucket"] = bucket
    uploadDetail["Key"] = file_key
    uploadDetail["UploadId"] = upload_id

    local uploader = s3_multi_upload:new(s3.auth, s3.host, s3.timeout, s3.ssl, s3.ssl_verify, uploadDetail)

    local transfer_id,chunk_size,chunk_index = string.match(ngx.var.uri,".+%-chunking%-(%d+)%-(%d+)%-(%d+)$")
    local ok, upload_res = uploader:upload(chunk_index + 1 ,req_reader, s3_req_header)
    if not ok then
        ngx.log(ngx.ERR,"upload [" .. file_key .. "] part to s3 failed! ")
        ngx.status = ngx.HTTP_INTERNAL_SERVER_ERROR
        return
    end

    if chunk_index + 1 < tonumber(chunk_size) then
        return
    end
else
    if tonumber(content_length) >= AUTO_CHUNK_SIZE then
        local co_wrap = function(func)
            local co = coroutine.create(func)
            if not co then
                return nil, "could not create coroutine"
            else
                return function(...)
                    if coroutine.status(co) == "suspended" then
                        return select(2, coroutine.resume(co, ...))
                    else
                        return nil, "can't resume a " .. coroutine.status(co) .. " coroutine"
                    end
                end
            end
        end

        local s3_req_header = get_s3_header()
        s3_req_header['Content-Length'] = 0
        local ok, uploader = s3:start_multi_upload(file_key, s3_req_header)
        if not ok then
            ngx.log(ngx.ERR,"start_multi_upload " .. cjson.encode({key=file_key, myheaders=s3_req_header}) .. "] failed! resp:" .. tostring(uploader))
            ngx.status = ngx.HTTP_INTERNAL_SERVER_ERROR
            return
        end

        local total_part_number = math.floor(content_length / CHUNK_SIZE)
        if content_length % CHUNK_SIZE > 0 then
            total_part_number = total_part_number + 1
        end
        local part_number = 1
        
        while part_number <= total_part_number do
            local chunk_left = CHUNK_SIZE
            if part_number == total_part_number then
                chunk_left = content_length % CHUNK_SIZE
            end
            s3_req_header = get_s3_header()
            s3_req_header["Host"] = s3.host
            s3_req_header['Content-Length'] = chunk_left
            local chunk_reader = co_wrap(function()
                while chunk_left > 0 do
                    coroutine.yield(req_reader(BUFFER_SIZE))
                    chunk_left = chunk_left - BUFFER_SIZE
                end
            end)
            local ok, upload_res = uploader:upload(part_number, chunk_reader, s3_req_header)
            if not ok then
                ngx.log(ngx.ERR,"upload [" .. file_key .. "] part to s3 failed! resp:" .. tostring(upload_res))
                ngx.status = ngx.HTTP_INTERNAL_SERVER_ERROR
                return
            end
            part_number = part_number + 1
        end
        s3_req_header = get_s3_header()
        s3_req_header["Host"] = s3.host
        local ok, complete_res = uploader:complete(s3_req_header)
        if not ok then
            ngx.log(ngx.ERR,"uploader:complete " .. cjson.encode({key=key, part_number=part_number, value=value}) .. "] failed! resp:" .. tostring(complete_res))
            ngx.status = ngx.HTTP_INTERNAL_SERVER_ERROR
            return
        end
    else
        local s3_req_header = get_s3_header()
        s3_req_header["Content-Length"] = content_length
        local ok, s3_res = s3:put(file_key, req_reader, s3_req_header)
        if not ok then
            ngx.log(ngx.ERR, "upload [" .. file_key .. "] direct to s3 failed! Response Header: ", cjson.encode(s3_res.headers))
            ngx.status = ngx.HTTP_INTERNAL_SERVER_ERROR
            return
        end
    end
end

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
