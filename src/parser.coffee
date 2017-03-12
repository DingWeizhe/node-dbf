{EventEmitter} = require 'events'
Header = require './header'
fs = require 'fs'
iconv = require 'iconv-lite'

class Parser extends EventEmitter

    constructor: (@filename, @options = {}) ->
        @encoding = @options?.encoding || 'utf-8'
        @start = @options?.start || 0
        @length = @options?.length || -1

    parse: =>
        @emit 'start', @

        @header = new Header @filename, @encoding
        @header.parse (err) =>

            @emit 'header', @header

            sequenceNumber = @start
            
            loc = 0
            bufLoc = 0
            overflow = null
            @paused = false
            
            stream = fs.createReadStream @filename, { start: @header.start + @start * @header.recordLength }

            numberOfRecords = @header.numberOfRecords
            if @length isnt -1 then numberOfRecords = @length

            @readBuf = =>
            
                if @paused
                    @emit 'paused'
                    return
                
                while buffer = stream.read()
                    if overflow isnt null then buffer = Buffer.concat([overflow, buffer])

                    while loc + bufLoc < (numberOfRecords * @header.recordLength) && (bufLoc + @header.recordLength) <= buffer.length
                        @emit 'record', @parseRecord ++sequenceNumber, buffer.slice bufLoc, bufLoc += @header.recordLength

                    loc += bufLoc
                    if bufLoc < buffer.length then overflow = buffer.slice bufLoc, buffer.length else overflow = null

                    return @
                    
            stream.on 'readable',@readBuf            
            stream.on 'end', () =>
                @emit 'end'

        return @
        
    pause: =>        
        @paused = true
        
    resume: =>    
        @paused = false        
        @emit 'resuming'        
        do @readBuf

    parseRecord: (sequenceNumber, buffer) =>
        record = {
            '@sequenceNumber': sequenceNumber
            '@deleted': (buffer.slice 0, 1)[0] isnt 32
        }

        loc = 1
        for field in @header.fields
            do (field) =>
                record[field.name] = @parseField field, buffer.slice loc, loc += field.length

        return record

    parseField: (field, buffer) =>
        value = iconv.decode(buffer, @encoding).trim()

        if field.type is 'N'
            value = parseInt value, 10
        else if field.type is 'F'
            value = if value == +value and value == (value | 0) then parseInt(value, 10) else parseFloat(value, 10)

        return value

module.exports = Parser
