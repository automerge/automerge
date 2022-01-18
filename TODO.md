### next steps:
  1. C API
  2. port rust command line tool
  3. fast load

### ergonomics:
  1. value() -> () or something that into's a value

### automerge:
  1. single pass (fast) load
  2. micro-patches / bare bones observation API / fully hydrated documents

### future:
  1. handle columns with unknown data in and out
  2. branches with different indexes

### Peritext
  1. add mark / remove mark -- type, start/end elemid (inclusive,exclusive)
  2. track any formatting ops that start or end on a character
  3. ops right before the character, ops right after that character
  4. query a single character - character, plus marks that start or end on that character
     what is its current formatting,
     what are the ops that include that in their span,
     None = same as last time, Set( bold, italic ),
     keep these on index
  5. op probably belongs with the start character - possible packed at the beginning or end of the list

### maybe:
  1. tables

### no:
  1. cursors
