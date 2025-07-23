// =============================================================================
// FILE 1: api/search.js (Railway Function - Cache-Only)
// =============================================================================

import { createClient } from '@supabase/supabase-js';

export default async function handler(req, res) {
    const startTime = Date.now();
    
    // CORS headers for VC integration
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-API-Key');
    
    if (req.method === 'OPTIONS') {
        return res.status(200).end();
    }
    
    if (req.method !== 'POST') {
        return res.status(405).json({ 
            error: 'Method not allowed',
            message: 'Use POST for property search'
        });
    }
    
    // API Key authentication
    const apiKey = req.headers['x-api-key'] || req.body.apiKey;
    if (!apiKey || apiKey !== process.env.VC_API_KEY) {
        return res.status(401).json({
            error: 'Unauthorized',
            message: 'Valid X-API-Key header required'
        });
    }
    
    try {
        const {
            neighborhood,
            propertyType = 'rental',
            bedrooms,
            bathrooms,
            undervaluationThreshold = 15,
            minPrice,
            maxPrice,
            maxResults = 1,
            noFee = false
        } = req.body;
        
        if (!neighborhood) {
            return res.status(400).json({
                error: 'Bad Request',
                message: 'neighborhood parameter is required',
                example: 'bushwick, soho, tribeca, williamsburg'
            });
        }
        
        console.log(`🔍 Railway Function: Searching cache for ${neighborhood}...`);
        
        // Initialize Supabase
        const supabase = createClient(
            process.env.SUPABASE_URL,
            process.env.SUPABASE_ANON_KEY
        );
        
        // Smart cache search
        const cacheResults = await searchCache(supabase, {
            neighborhood: neighborhood.toLowerCase().replace(/\s+/g, '-'),
            propertyType,
            bedrooms,
            bathrooms,
            undervaluationThreshold,
            minPrice,
            maxPrice,
            maxResults: Math.min(parseInt(maxResults), 10),
            noFee
        });
        
        const processingTime = Date.now() - startTime;
        
        if (cacheResults.length > 0) {
            // CACHE HIT - Return instantly with Instagram formatting
            console.log(`✅ Cache hit: ${cacheResults.length} properties found in ${processingTime}ms`);
            
            return res.json({
                success: true,
                source: 'cache_hit',
                data: {
                    properties: cacheResults,
                    instagramReady: formatForInstagram(cacheResults),
                    instagramSummary: {
                        hasImages: cacheResults.some(p => p.image_count > 0),
                        totalImages: cacheResults.reduce((sum, p) => sum + (p.image_count || 0), 0),
                        primaryImages: cacheResults.map(p => p.primary_image).filter(Boolean),
                        readyForPosting: cacheResults.filter(p => p.image_count > 0 && p.primary_image)
                    },
                    summary: {
                        totalFound: cacheResults.length,
                        cacheHits: cacheResults.length,
                        newlyScraped: 0,
                        thresholdUsed: undervaluationThreshold,
                        thresholdLowered: false,
                        processingTimeMs: processingTime,
                        responseType: 'instant_cache'
                    }
                },
                message: `Found ${cacheResults.length} properties from cache (instant results!)`,
                processingTime: `${processingTime}ms`,
                fallbackAvailable: true,
                fallbackUrl: `${process.env.FULL_API_URL}/api/search/smart`
            });
        } else {
            // CACHE MISS - Suggest full API
            console.log(`❌ Cache miss for ${neighborhood} with criteria`);
            
            return res.json({
                success: true,
                source: 'cache_miss',
                data: {
                    properties: [],
                    instagramReady: [],
                    instagramSummary: {
                        hasImages: false,
                        totalImages: 0,
                        primaryImages: [],
                        readyForPosting: []
                    },
                    summary: {
                        totalFound: 0,
                        cacheHits: 0,
                        newlyScraped: 0,
                        processingTime: processingTime,
                        responseType: 'cache_miss'
                    }
                },
                message: 'No cached results found. Use full API for fresh scraping.',
                processingTime: `${processingTime}ms`,
                fallbackRequired: true,
                fallbackUrl: `${process.env.FULL_API_URL}/api/search/smart`,
                fallbackInstructions: {
                    method: 'POST',
                    headers: { 'X-API-Key': 'same_api_key' },
                    body: req.body,
                    expectedTime: '2-5 minutes for fresh analysis'
                }
            });
        }
        
    } catch (error) {
        console.error('Railway Function error:', error);
        return res.status(500).json({
            error: 'Internal Server Error',
            message: 'Cache search failed',
            details: error.message,
            fallbackUrl: `${process.env.FULL_API_URL}/api/search/smart`
        });
    }
}

// Cache search function
async function searchCache(supabase, params) {
    try {
        const tableName = params.propertyType === 'rental' ? 'undervalued_rentals' : 'undervalued_sales';
        const priceColumn = params.propertyType === 'rental' ? 'monthly_rent' : 'price';
        const cacheMaxAgeDays = 30;
        const cutoffDate = new Date(Date.now() - (cacheMaxAgeDays * 24 * 60 * 60 * 1000));

        let query = supabase
            .from(tableName)
            .select('*')
            .eq('neighborhood', params.neighborhood)
            .eq('status', 'active')
            .gte('discount_percent', params.undervaluationThreshold)
            .gte('created_at', cutoffDate.toISOString());

        if (params.bedrooms !== undefined) {
            query = query.eq('bedrooms', params.bedrooms);
        }
        if (params.bathrooms !== undefined) {
            query = query.eq('bathrooms', params.bathrooms);
        }
        if (params.minPrice !== undefined) {
            query = query.gte(priceColumn, params.minPrice);
        }
        if (params.maxPrice !== undefined) {
            query = query.lte(priceColumn, params.maxPrice);
        }
        if (params.noFee && params.propertyType === 'rental') {
            query = query.eq('no_fee', true);
        }

        const { data, error } = await query
            .order('discount_percent', { ascending: false })
            .limit(params.maxResults);

        if (error) throw error;
        return data || [];

    } catch (error) {
        console.error('Cache search error:', error);
        return [];
    }
}

// Instagram formatting function
function formatForInstagram(properties) {
    return properties.map(property => ({
        ...property,
        source: 'cache',
        isCached: true,
        instagram: {
            primaryImage: property.primary_image,
            imageCount: property.image_count || 0,
            images: property.instagram_ready_images || [],
            dmMessage: generateInstagramDMMessage(property)
        }
    }));
}

// Instagram DM message generator
function generateInstagramDMMessage(property) {
    const price = property.monthly_rent || property.price;
    const priceText = property.monthly_rent ? `$${price?.toLocaleString()}/month` : `$${price?.toLocaleString()}`;
    const savings = property.potential_monthly_savings || property.potential_savings;
    
    let message = `🏠 *UNDERVALUED PROPERTY ALERT*\n\n`;
    message += `📍 **${property.address}**\n`;
    message += `🏘️ ${property.neighborhood}, ${property.borough}\n\n`;
    message += `💰 **${priceText}**\n`;
    message += `📉 ${property.discount_percent}% below market\n`;
    message += `💵 Save $${savings?.toLocaleString()} ${property.monthly_rent ? 'per month' : 'total'}\n\n`;
    message += `🏠 ${property.bedrooms}BR/${property.bathrooms}BA`;
    if (property.sqft) message += ` | ${property.sqft} sqft`;
    message += `\n📊 Score: ${property.score}/100 (${property.grade})\n\n`;
    
    // Add key amenities
    const keyAmenities = [];
    if (property.no_fee) keyAmenities.push('No Fee');
    if (property.doorman_building) keyAmenities.push('Doorman');
    if (property.elevator_building) keyAmenities.push('Elevator');
    if (property.pet_friendly) keyAmenities.push('Pet Friendly');
    if (property.gym_available) keyAmenities.push('Gym');
    
    if (keyAmenities.length > 0) {
        message += `✨ ${keyAmenities.join(' • ')}\n\n`;
    }
    
    message += `⚡ *Instant cache result*\n`;
    message += `🔗 [View Full Listing](${property.listing_url})`;
    
    return message;
}
