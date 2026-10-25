'use strict';
// Discovery references identify candidates, not performance or authenticated access.
const VERSION='source-universe-v1.0.0';
const rows=[
 ['telegram-front-runners','Front Runners','frontrunz','AMPLIFIER','front-runners','front runners|front run'],
 ['pows-gem-calls',"Pow’s Gem Calls",'PowsGemCalls','AMPLIFIER','pow','pow'],
 ['madapes-gambles','MadApes Gambles','mad_apes_gambles','AMPLIFIER','madapes','madapes'],
 ['madapes-calls','MadApes Calls','mad_apes_call','AMPLIFIER','madapes','madapes'],
 ['bagcalls','BagCalls','bagcalls','AMPLIFIER','bagcalls','bagcalls'],
 ['marcells-trenches',"Marcell’s Trenches",'marcellcooks','AMPLIFIER','marcell','marcell'],
 ['whale-wallet-watchers','Whale Wallet Watchers','WhaleWalletWatchers','DISCOVERY','whale-wallet-watchers','whale wallet watchers'],
 ['spidercrypto-journal','SpiderCrypto Trading Journal','spidersjournal','DISCOVERY','spidercrypto','spidercrypto'],
 ['prints-press','Prints Printing Press','printspress','DISCOVERY','prints','prints'],
 ['gabbens-calls','Gabbens Calls','GabbensCalls','DISCOVERY','gabbens','gabben'],
 ['xanders-trenches',"Xander’s Trenches",'xanderstrenches','DISCOVERY','xander','xander'],
 ['ourcryptohood','OurCryptoHood Calls',null,'DISCOVERY','ourcryptohood',null],
 ['call-analyser','Call Analyser','CallAnalyser','MACHINE_INTELLIGENCE','call-analyser','call analyser'],
 ['call-analyser-sol','Call Analyser (SOL Only)','CallAnalyserSol','MACHINE_INTELLIGENCE','call-analyser','call analyser'],
 ['wat-is-des','wat is des','watisdes','DISCOVERY','wat-is-des','wat is des'],
 ['multichain1000x','1000X Multi Chain','MultiChain1000x','DISCOVERY','multichain1000x','multi chain|multi.?chain'],
 ['based-kook','based kook calls','hellokook','DISCOVERY','based-kook','kook'],
 ['house-of-degeneracy',"Iced’s House of Degeneracy",'houseofdegeneracy','DISCOVERY','iced','degeneracy'],
 ['sugar-calls','Sugar Calls','sugarydick','DISCOVERY','sugar','sugar'],
 ['spiders-degens','Spider’s Degens','spidersdegens','DISCOVERY','spidercrypto','spider'],
 ['bags-casino','Bags Casino','Bagscasino','DISCOVERY','bagcalls','bag'],
];
const SOURCES=rows.map(([sourceId,displayName,username,sourceRole,clusterId,titlePattern])=>({sourceId,displayName,username,
 sourceRole,clusterId,titlePattern,universeVersion:VERSION,platform:'telegram',clusterRelationshipStatus:'UNKNOWN',
 identityStatus:'CANDIDATE',activation:'REQUIRES_AUTHENTICATED_PUBLIC_CHANNEL_READ',
 identityEvidence:username?`https://t.me/${username}`:null,
 discoveryReference:['ourcryptohood','whale-wallet-watchers','prints-press','gabbens-calls','wat-is-des','multichain1000x','based-kook','house-of-degeneracy','sugar-calls'].includes(sourceId)?'https://kolspace.io/leaderboard':null}));
const RELATIONSHIPS=[
 {a:'madapes-calls',b:'madapes-gambles',status:'CORRELATED',evidence:'https://t.me/s/mad_apes_call',reason:'Main channel links its Gambles channel.'},
 {a:'call-analyser',b:'call-analyser-sol',status:'CORRELATED',evidence:'https://t.me/s/CallAnalyserSol',reason:'Same machine intelligence publisher.'},
 {a:'spidercrypto-journal',b:'spiders-degens',status:'CORRELATED',evidence:'https://t.me/s/spidersjournal',reason:'Journal links its degens channel.'},
 {a:'bagcalls',b:'bags-casino',status:'CORRELATED',evidence:'https://t.me/s/bagcalls',reason:'Main channel links its Gambles channel.'},
];
function candidateSources(){return SOURCES.map(s=>({...s}));}
function relationshipGraph(env=process.env){
 if(!env.SIGNAL_OPPORTUNITY_RELATIONSHIPS_JSON)return {version:'relationship-graph-v1.0.0',edges:RELATIONSHIPS};
 const graph=JSON.parse(env.SIGNAL_OPPORTUNITY_RELATIONSHIPS_JSON),ids=new Set(SOURCES.map(s=>s.sourceId));
 if(!/^relationship-graph-v\d+\.\d+\.\d+$/.test(graph.version||'')||graph.version==='relationship-graph-v1.0.0'||!Array.isArray(graph.edges)||graph.edges.length>250)throw Error('invalid_relationship_graph_version');
 for(const e of graph.edges)if(!ids.has(e.a)||!ids.has(e.b)||e.a===e.b||!['INDEPENDENT','CORRELATED','UNKNOWN'].includes(e.status)
  ||typeof e.evidence!=='string'||!e.evidence.startsWith('https://')||!e.reviewedBy||!Number.isFinite(Date.parse(e.knowledgeAt)))throw Error('relationship_review_evidence_required');
 return {version:graph.version,edges:[...RELATIONSHIPS,...graph.edges]};
}
module.exports={VERSION,SOURCES,RELATIONSHIPS,candidateSources,relationshipGraph};
