// =============================================================================
// FILE 2: api/health.js (Railway Function - Health Check)
// =============================================================================

export default async function handler(req, res) {
    res.setHeader('Access-Control-Allow-Origin', '*');
    
    return res.json({
        status: 'healthy',
        service: 'railway_functions_cache',
        timestamp: new Date().toISOString(),
        version: '1.0.0',
        mode: 'cache_first_with_full_api_fallback',
        features: [
            'instant_cache_lookup',
            'instagram_dm_ready', 
            'full_api_fallback'
        ]
    });
}
