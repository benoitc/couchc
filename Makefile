COUCHDB_SRC?= ../couchdb

ERL_COMPILER_OPTIONS="[{i, \"$(COUCHDB_SRC)/src/couchdb\"}]"

all:
	@env ERL_COMPILER_OPTIONS=$(ERL_COMPILER_OPTIONS) rebar compile

clean:
	@rebar clean
