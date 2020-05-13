
CC=gcc
CFLAGS=-I.
DEPS=automerge.h
LIBS=-lautomerge -lpthread -ldl -lm
LDIR=../target/release
LIB=../target/release/libautomerge.a
DEBUG_LIB=../target/debug/libautomerge.a

all: automerge $(LIB)

debug: LDIR=../target/debug
debug: automerge $(DEBUG_LIB)

automerge: automerge.o $(LDIR)/libautomerge.a
	$(CC) -o $@ automerge.o $(LIBS) -L$(LDIR)

$(DEBUG_LIB): src/lib.rs
	cargo build

$(LIB): src/lib.rs
	cargo build --release

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

.PHONY: clean

clean:
	rm -f *.o automerge $(LIB) $(DEBUG_LIB)
