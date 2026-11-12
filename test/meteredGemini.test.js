'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { createGeminiClient, GeminiApiError, API_ORIGIN } = require('../utils/meteredGemini');
const { estimateGeminiReservation, estimateGeminiUsage } = require('../utils/apiPricing');

const params = { model: 'gemini-2.5-flash-lite', body: {
  systemInstruction: { parts: [{ text: 'Phrase verified evidence only.' }] },
  contents: [{ role: 'user', parts: [{ text: 'Verified fixture.' }] }],
  generationConfig: { maxOutputTokens: 900, thinkingConfig: { thinkingBudget: 0 } },
} };

test('Gemini writer uses the dedicated key header, bounded request and usage ledger', async () => {
  const calls=[],reserved=[],settled=[];
  const fetchImpl=async (url, options) => {
    calls.push({url,options});
    return {ok:true,status:200,headers:{get:()=>null},json:async()=>({
      candidates:[{content:{parts:[{text:'Grounded result.'}]}}],
      usageMetadata:{promptTokenCount:100,candidatesTokenCount:20,totalTokenCount:120},
    })};
  };
  const client=createGeminiClient({apiKey:'fixture-google-key',fetchImpl,
    reserve:async metadata=>{reserved.push(metadata);return 'ledger-id';},
    settle:async (id,outcome)=>settled.push({id,outcome})});
  const response=await client.generateContent(params,{timeout:30000,apiUsageContext:{tenantId:'21'}});
  assert.equal(response.candidates[0].content.parts[0].text,'Grounded result.');
  assert.equal(calls[0].url,`${API_ORIGIN}/v1beta/models/gemini-2.5-flash-lite:generateContent`);
  assert.equal(calls[0].options.headers['x-goog-api-key'],'fixture-google-key');
  assert.equal(JSON.parse(calls[0].options.body).generationConfig.maxOutputTokens,900);
  assert.equal(reserved[0].provider,'google_gemini'); assert.equal(reserved[0].tenantId,'21');
  assert.equal(reserved[0].pricingBasis,'gemini_2_5_flash_lite_standard_2026_11_12');
  assert.equal(settled[0].outcome.status,'ok'); assert.deepEqual(settled[0].outcome.usage,{input_tokens:100,output_tokens:20,total_tokens:120});
  assert.doesNotMatch(JSON.stringify(reserved)+JSON.stringify(settled),/fixture-google-key/);
});

test('provider error retains only the structured envelope needed by sanitized diagnostics', async () => {
  const settle=[];
  const client=createGeminiClient({apiKey:'fixture-google-key',
    fetchImpl:async()=>({ok:false,status:400,headers:{get:key=>key==='x-request-id'?'request-123':null},json:async()=>({error:{
      code:400,status:'INVALID_ARGUMENT',message:'sensitive provider message',details:[],
    }})}), reserve:async()=> 'ledger-id', settle:async(id,outcome)=>settle.push({id,outcome})});
  await assert.rejects(client.generateContent(params,{timeout:30000}), err => {
    assert.ok(err instanceof GeminiApiError); assert.equal(err.status,400); assert.equal(err.requestID,'request-123'); return true;
  });
  assert.equal(settle[0].outcome.status,'uncertain'); assert.equal(settle[0].outcome.httpStatus,400);
});

test('verified Flash-Lite rates bound reservations and actual usage', () => {
  const reservation=estimateGeminiReservation(params);
  assert.ok(reservation > 0 && reservation < 0.002);
  assert.equal(estimateGeminiUsage('gemini-2.5-flash-lite',{promptTokenCount:1_000_000,candidatesTokenCount:1_000_000,totalTokenCount:2_000_000}),0.5);
  assert.equal(estimateGeminiUsage('unknown',{}),null);
});
