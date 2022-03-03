
### Some ideas of what this could look like

```bash
  $ automege export foo.mpl
  {
     "name": "bob",
     "numbers": [ 1,2,3,4 ]
  }
  $ automerge export --format toml foo.mpl
  name = "bob"
  numbers = [
    1.0,
    2.0,
    3.0,
    4.0
  ]
  $ automerge import foo.json --out bar.mpl
  $ automerge export foo.mpl | automerge import - > bar.mpl
  $ automerge set foo.mpl "birds[3].name" "wren"
  $ cat wren.json | automerge set foo.mpl "birds[3]"
  $ automerge get foo.mpl "birds[3].name"
  $ automerge union foo.mpl bar.mpl --out baz.mpl
  $ automerge intersect foo.mpl bar.mpl --out baz.mpl
  $ automerge union foo.mpl bar.mpl - > baz.mpl
  $ cat foo.mpl bar.mpl | automerge union --out baz.mpl
  $ cat foo.mpl bar.mpl | automerge union - > baz.mpl
  $ automerge status foo.mpl
    1022 changes, 10942 operations, 47 objects
    created: 2:31pm April 4, 2020
    actors:
      1111111: 124 changes, 1:01am April 6, 2020
      2222222: 457 changes, 8:10pm April 4, 2020
      3333333: 590 changes, 10:01pm May 2, 2020
  $ automerge log foo.mpl --actor 11111111
    aa88f76 : [ ] : Jan 1, 20202, "first commit" 
    87fa8c1 : [ aa88f76, 971651 ] : Jan 2, 20202, "seccond commit"
    776aa5c : [ 87fa8c1 ] : Jan 2, 20202, "third commit"  
  $ automerge fork 776aa5c foo.mpl --out bar.mpl
  $ automerge fork 776aa5c - < foo.mpl > bar.mpl
  $ automerge diff foo.mpl bar.mpl
  182 changes in common:
    + 87c162 
    + 97ac42 
    - ffac11 
    - 1adaf1 
  {
  - "name":"bob",
  + "name":"joe",
    "numbers": [
  -   1,
  -   2,
  +   3,
      4
     ]
  }
  
```   
