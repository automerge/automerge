// Here's the JS:
//
// action: [{"type":"splitBlock","index":3,"value":{"type":"hFKlnzj","parents":["8E","ezvnAss"],"attrs":{"ff5g":" wJZ4u"}}},{"type":"delete","index":1,"length":0},{"type":"addMark","range":{"start":0,"end":1,"expand":"after"},"name":"bold","value":true},{"type":"addMark","range":{"start":0,"end":3,"expand":"after"},"name":"anchor","value":"left"},{"type":"splitBlock","index":0,"value":{"type":"_","parents":[],"attrs":{"0":[[" "],"A"],"H":[]}}}]
// reproducing test case:

// const spansBefore: am.Span[] = [
// {"type":"block","value":{"type":"p","parents":["p","p","p","unordered-list","unordered-list"],"attrs":{"italic":"right","anchor":-1940384460,"bold":false}}} ,
// {"type":"block","value":{"type":"p","parents":["p","unordered-list"],"attrs":{"bold":-1169315486,"italic":"left","anchor":false}}} ,
// {"type":"block","value":{"type":"ordered-list","parents":["ordered-list","unordered-list"],"attrs":{"anchor":"left","bold":"right","italic":"right"}}} ,
// {"type":"block","value":{"type":"p","parents":["ordered-list","ordered-list"],"attrs":{"bold":"left","anchor":true,"italic":-2019963358}}} ,
// {"type":"text","value":"2Tug"} ,
// ]
// const spansAfter: am.Span[] = [
// {"type":"block","value":{"attrs":{"0":[[" "],"A"],"H":[]},"type":"_","parents":[]}} ,
// {"type":"block","value":{"attrs":{"italic":"right","anchor":-1940384460,"bold":false},"parents":["p","p","p","unordered-list","unordered-list"],"type":"p"}} ,
// {"type":"block","value":{"type":"p","parents":["p","unordered-list"],"attrs":{"bold":-1169315486,"italic":"left","anchor":false}}} ,
// {"type":"block","value":{"attrs":{"bold":"right","italic":"right","anchor":"left"},"parents":["ordered-list","unordered-list"],"type":"ordered-list"}} ,
// {"type":"block","value":{"type":"hFKlnzj","parents":["8E","ezvnAss"],"attrs":{"ff5g":" wJZ4u"}}} ,
// {"type":"block","value":{"type":"p","attrs":{"italic":-2019963358,"anchor":true,"bold":"left"},"parents":["ordered-list","ordered-list"]}} ,
// {"type":"text","value":"2Tug"} ,
// ]
// const patches: am.Patch[] = [
// {"action":"insert","path":["text",0],"values":[{}]} ,
// {"action":"mark","path":["text"],"marks":[{"name":"anchor","value":"left","start":1,"end":4},{"name":"bold","value":true,"start":1,"end":2}]} ,
// {"action":"insert","path":["text",4],"values":[{}]} ,
// {"action":"put","path":["text",4,"attrs"],"value":{}} ,
// {"action":"put","path":["text",4,"parents"],"value":[]} ,
// {"action":"put","path":["text",4,"type"],"value":""} ,
// {"action":"put","path":["text",4,"attrs","ff5g"],"value":" wJZ4u"} ,
// {"action":"insert","path":["text",4,"parents",0],"values":["8E","ezvnAss"]} ,
// {"action":"splice","path":["text",4,"type",0],"value":"hFKlnzj"} ,
// {"action":"put","path":["text",0,"attrs"],"value":{}} ,
// {"action":"put","path":["text",0,"parents"],"value":[]} ,
// {"action":"put","path":["text",0,"type"],"value":""} ,
// {"action":"splice","path":["text",0,"type",0],"value":"_"} ,
// {"action":"put","path":["text",0,"attrs","0"],"value":[]} ,
// {"action":"put","path":["text",0,"attrs","H"],"value":[]} ,
// {"action":"insert","path":["text",0,"attrs","0",0],"values":["A",[]]} ,
// {"action":"insert","path":["text",0,"attrs","0",0,0],"values":[" "]} ,
// ]
//
// It can be a little tricky to see what's wrong here. Basically, if you look at the final splitblock action it's this:
//
// {"type":"splitBlock","index":0,"value":{"type":"_","parents":[],"attrs":{"0":[[" "],"A"],"H":[]}}}
//
// But if you look at the patch for this attribute it's these
//
// {"action":"put","path":["text",0,"attrs","0"],"value":[]} ,
// {"action":"insert","path":["text",0,"attrs","0",0],"values":["A",[]]} ,
// {"action":"insert","path":["text",0,"attrs","0",0,0],"values":[" "]} ,
//
// You can see here that the order of the attributes is wrong. The "A" should be
// first, then the empty array (which a " " is later inserted into). But instead
// it's the other way around.
//
// I need to reproduce this in a rust test case and then figure out what's happening
