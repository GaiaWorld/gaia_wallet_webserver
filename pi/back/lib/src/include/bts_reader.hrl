-record(bts_reader_context, {
							self,
							checksum,
							deserializer,
							file,
							fd,
							buffer,
							buffer_sizeout,
							buffer_timeout,
							timer_ref
}).
-define(BTS_READER_HIBERNATE_TIMEOUT, 3000).
-define(FILE_READ, read).
-define(INIT_FD_COUNT, 0).
-define(DESERIALIZER, deserializer).
-define(DEFAULT_DESERIALIZER, {bts_reader, []}).
-define(READ_BUFFER_SIZEOUT, sizeout).
-define(DEFAULT_READ_BUFFER_SIZEOUT, 30).
-define(READ_BUFFER_TIMEOUT, timeout).
-define(DEFAULT_READ_BUFFER_TIMEOUT, 5).
