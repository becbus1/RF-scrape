// optimal-weekly-streeteasy.js
// ENHANCED VERSION: Correct API endpoints with full image extraction

require('dotenv').config();
const axios = require('axios');
const { createClient } = require('@supabase/supabase-js');

const VALID_STREETEASY_SLUGS = new Set([
    "west-village", "east-village", "soho", "tribeca", "chelsea",
    "upper-east-side", "upper-west-side", "financial-district", "lower-east-side",
    "gramercy-park", "murray-hill", "hells-kitchen", "midtown",
    "park-slope", "williamsburg", "dumbo", "brooklyn-heights", "fort-greene",
    "prospect-heights", "crown-heights", "bedford-stuyvesant", "greenpoint",
    "red-hook", "carroll-gardens", "bushwick", "sunset-park",
    "long-island-city", "hunters-point", "astoria", "sunnyside",
    "woodside", "jackson-heights", "forest-hills", "kew-gardens",
    "mott-haven", "concourse", "fordham", "riverdale",
    "saint-george", "stapleton", "new-brighton"
]);

const { HIGH_PRIORITY_NEIGHBORHOODS } = require('./comprehensive-nyc-neighborhoods.js');

class OptimalWeeklyStreetEasy {
    constructor() {
        this.supabase = createClient(
            process.env.SUPABASE_URL,
            process.env.SUPABASE_ANON_KEY
        );
        
        this.rapidApiKey = process.env.RAPIDAPI_KEY;
        this.apiCallsUsed = 0;
        
        // AGGRESSIVE rate limiting settings
        this.baseDelay = 15000; // 15 seconds between calls
        this.maxRetries = 3;
        this.retryBackoffMultiplier = 2; // 15s, 30s, 60s on retries
    }

    /**
     * Main weekly refresh with aggressive rate limiting and duplicate prevention
     */
    async runWeeklyUndervaluedRefresh() {
        console.log('\n🗽 Starting ENHANCED Weekly StreetEasy Analysis with Images');
        console.log('⏱️ Using correct API endpoints with 15+ second delays');
        console.log('🔧 Enhanced: Full image extraction from StreetEasy responses');
        console.log('🖼️ Direct image display - Zero bandwidth cost');
        console.log('🔒 Duplicate prevention enabled');
        console.log('='.repeat(60));

        const summary = {
            startTime: new Date(),
            neighborhoodsProcessed: 0,
            totalPropertiesFetched: 0,
            undervaluedFound: 0,
            savedToDatabase: 0,
            updatedInDatabase: 0,
            duplicatesSkipped: 0,
            propertiesWithImages: 0,
            totalImagesExtracted: 0,
            apiCallsUsed: 0,
            errors: [],
            rateLimitHits: 0
        };

        try {
            // Clear old data
            await this.clearOldUndervaluedProperties();

            // Get valid neighborhoods only
            const validNeighborhoods = this.getValidNeighborhoods();
            console.log(`🎯 Processing ${validNeighborhoods.length} valid neighborhoods with 15s+ delays\n`);

            // Process each neighborhood with aggressive spacing
            for (let i = 0; i < validNeighborhoods.length; i++) {
                const neighborhood = validNeighborhoods[i];
                
                try {
                    console.log(`🔍 [${i + 1}/${validNeighborhoods.length}] Processing ${neighborhood}...`);
                    
                    // Apply base delay before each call (except first)
                    if (i > 0) {
                        const delay = this.calculateDelay(i);
                        console.log(`   ⏰ Waiting ${delay/1000}s to avoid rate limits...`);
                        await this.delay(delay);
                    }
                    
                    const properties = await this.fetchNeighborhoodPropertiesWithRetry(neighborhood);
                    
                    // NEW: Also fetch past sales for better comparables
                    const pastSales = await this.fetchPastSalesComparables(neighborhood);
                    console.log(`   📊 Past sales data: ${pastSales.length} recent sales found`);
                    
                    summary.totalPropertiesFetched += properties.length;
                    summary.apiCallsUsed++;

                    // Count properties with images
                    const propertiesWithImages = properties.filter(p => p.image_url || (p.image_urls && p.image_urls.length > 0));
                    summary.propertiesWithImages += propertiesWithImages.length;
                    summary.totalImagesExtracted += properties.reduce((sum, p) => sum + (p.image_count || 0), 0);

                    console.log(`   🖼️ Images extracted: ${propertiesWithImages.length}/${properties.length} properties have images`);

                    if (properties.length > 0) {
                        const undervalued = this.filterUndervaluedProperties(properties, neighborhood, pastSales);
                        summary.undervaluedFound += undervalued.length;

                        if (undervalued.length > 0) {
                            const saveResult = await this.saveUndervaluedPropertiesWithStats(undervalued, neighborhood);
                            summary.savedToDatabase += saveResult.newCount;
                            summary.updatedInDatabase += saveResult.updateCount;
                            summary.duplicatesSkipped += saveResult.duplicateCount;
                            console.log(`   ✅ ${neighborhood}: ${saveResult.newCount} new, ${saveResult.updateCount} updated, ${saveResult.duplicateCount} duplicates`);
                        } else {
                            console.log(`   📊 ${neighborhood}: ${properties.length} properties, none undervalued`);
                        }
                    } else {
                        console.log(`   📊 ${neighborhood}: No properties returned`);
                    }

                    summary.neighborhoodsProcessed++;

                } catch (error) {
                    const isRateLimit = error.response?.status === 429;
                    const is404 = error.response?.status === 404;
                    
                    if (isRateLimit) {
                        summary.rateLimitHits++;
                        console.error(`   ❌ RATE LIMITED on ${neighborhood} - increasing delays`);
                        
                        // Exponentially increase base delay after rate limit hits
                        this.baseDelay = Math.min(this.baseDelay * 1.5, 60000); // Max 60s
                        console.log(`   ⏰ New base delay: ${this.baseDelay/1000}s`);
                        
                        // Wait extra long after rate limit
                        await this.delay(this.baseDelay * 2);
                    } else if (is404) {
                        console.error(`   ❌ 404 ERROR on ${neighborhood} - endpoint/parameter issue`);
                    } else {
                        console.error(`   ❌ Error with ${neighborhood}: ${error.message}`);
                    }
                    
                    summary.errors.push({ 
                        neighborhood, 
                        error: error.message,
                        isRateLimit,
                        is404 
                    });
                }

                // Log progress every 5 neighborhoods
                if ((i + 1) % 5 === 0) {
                    const elapsed = (Date.now() - summary.startTime) / 1000 / 60;
                    console.log(`\n📊 Progress: ${i + 1}/${validNeighborhoods.length} neighborhoods (${elapsed.toFixed(1)}min elapsed)`);
                    console.log(`📊 Stats: ${summary.undervaluedFound} undervalued, ${summary.propertiesWithImages} with images, ${summary.rateLimitHits} rate limits\n`);
                }
            }

            summary.endTime = new Date();
            summary.duration = (summary.endTime - summary.startTime) / 1000 / 60;

            this.logWeeklySummary(summary);
            await this.saveWeeklySummary(summary);

        } catch (error) {
            console.error('💥 Weekly refresh failed:', error.message);
            summary.errors.push({ error: error.message });
        }

        return summary;
    }

    /**
     * Calculate progressive delay - gets longer as we make more calls
     */
    calculateDelay(callIndex) {
        // Start with base delay, increase gradually
        const progressiveIncrease = Math.floor(callIndex / 5) * 2000; // +2s every 5 calls
        return this.baseDelay + progressiveIncrease;
    }

    /**
     * Fetch with retry logic and exponential backoff
     */
    async fetchNeighborhoodPropertiesWithRetry(neighborhood) {
        let lastError;
        
        for (let attempt = 1; attempt <= this.maxRetries; attempt++) {
            try {
                return await this.fetchNeighborhoodProperties(neighborhood);
                
            } catch (error) {
                lastError = error;
                const isRateLimit = error.response?.status === 429;
                const is404 = error.response?.status === 404;
                
                if (is404) {
                    // Don't retry 404s - it's an endpoint/parameter issue
                    throw new Error(`404 Not Found - Check API endpoint and parameters for ${neighborhood}`);
                } else if (isRateLimit && attempt < this.maxRetries) {
                    const backoffDelay = this.baseDelay * Math.pow(this.retryBackoffMultiplier, attempt);
                    console.log(`   🔄 Rate limited (attempt ${attempt}), waiting ${backoffDelay/1000}s before retry...`);
                    await this.delay(backoffDelay);
                } else if (attempt < this.maxRetries) {
                    // For non-rate-limit errors, shorter delay
                    console.log(`   🔄 Error (attempt ${attempt}), waiting 5s before retry...`);
                    await this.delay(5000);
                } else {
                    // Final attempt failed
                    throw lastError;
                }
            }
        }
        
        throw lastError;
    }

    /**
     * ENHANCED: Correct API call format with comprehensive image extraction
     */
    async fetchNeighborhoodProperties(neighborhood) {
        console.log(`   📡 Calling API for ${neighborhood}...`);
        
        const response = await axios.get(
            'https://streeteasy-api.p.rapidapi.com/sales/search',
            {
                params: {
                    areas: neighborhood,  // Should be properly encoded by axios
                    limit: 500,          // Maximum allowed
                    minPrice: 200000,    // Reasonable minimum for NYC
                    maxPrice: 10000000,  // High maximum to catch all
                    offset: 0            // Start from beginning
                },
                headers: {
                    'X-RapidAPI-Key': this.rapidApiKey,
                    'X-RapidAPI-Host': 'streeteasy-api.p.rapidapi.com'
                },
                timeout: 30000
            }
        );

        console.log(`   📊 Raw response keys:`, Object.keys(response.data || {}));
        console.log(`   📊 Response status:`, response.status);
        
        // Handle response based on documentation format
        let propertiesData = [];
        
        if (response.data) {
            // Log the actual structure we received
            console.log(`   📋 Full response structure:`, JSON.stringify(response.data, null, 2).substring(0, 500) + '...');
            
            // Try different property arrays based on API docs
            if (response.data.results && Array.isArray(response.data.results)) {
                propertiesData = response.data.results;
                console.log(`   ✅ Found properties in 'results' array: ${propertiesData.length}`);
            }
            else if (response.data.listings && Array.isArray(response.data.listings)) {
                propertiesData = response.data.listings;
                console.log(`   ✅ Found properties in 'listings' array: ${propertiesData.length}`);
            }
            else if (Array.isArray(response.data)) {
                propertiesData = response.data;
                console.log(`   ✅ Response is direct array: ${propertiesData.length}`);
            }
            else if (response.data.properties && Array.isArray(response.data.properties)) {
                propertiesData = response.data.properties;
                console.log(`   ✅ Found properties in 'properties' array: ${propertiesData.length}`);
            }
            else {
                console.warn(`   ⚠️ Unexpected response format. Available keys:`, Object.keys(response.data));
                console.warn(`   ⚠️ First 200 chars of response:`, JSON.stringify(response.data).substring(0, 200));
                return [];
            }
            
            // Log pagination info if present
            if (response.data.pagination) {
                console.log(`   📄 Pagination: ${response.data.pagination.count} total, next offset: ${response.data.pagination.nextOffset}`);
            }
        }

        // FIELD DISCOVERY: Log all unique field names across all properties
        const allFieldNames = new Set();
        propertiesData.forEach(property => {
            Object.keys(property).forEach(key => allFieldNames.add(key));
        });
        
        console.log(`   🔍 ALL AVAILABLE FIELDS: ${Array.from(allFieldNames).sort().join(', ')}`);
        
        // Map to consistent format with comprehensive image extraction
        const mappedProperties = propertiesData.map((property, index) => {
            // Log first few property structures to understand the data
            if (index < 2) {
                console.log(`   🏠 Raw property ${index + 1} (showing ALL fields):`);
                console.log(JSON.stringify(property, null, 2));
            }
            
            // ENHANCED: Extract images with multiple fallback strategies
            const imageData = this.extractImageUrls(property);
            
            // Try multiple field mappings for better data extraction
            const mappedProperty = {
                listing_id: property.id || property.listing_id || property.streeteasy_id || `fallback-${Date.now()}-${index}`,
                address: property.address || property.street_address || property.full_address || property.location || 'Address not available',
                neighborhood: neighborhood,
                price: property.price || property.list_price || property.asking_price || property.listing_price || 0,
                sqft: property.sqft || property.square_feet || property.size || property.area || null,
                beds: property.beds || property.bedrooms || property.bed_count || property.num_beds || 0,
                baths: property.baths || property.bathrooms || property.bath_count || property.num_baths || 0,
                description: property.description || property.details || property.remarks || property.listing_description || '',
                url: property.url || property.link || property.streeteasy_url || property.listing_url || '',
                property_type: property.type || property.property_type || property.building_type || property.home_type || 'unknown',
                days_on_market: property.days_on_market || property.dom || property.days_on_redfin || property.days_listed || 0,
                fetched_date: new Date().toISOString(),
                
                // ENHANCED: Image data extraction
                image_url: imageData.primaryImageUrl,
                image_urls: imageData.allImageUrls,
                image_count: imageData.imageCount,
                
                // Additional fields that might be available
                building_name: property.building_name || property.building || '',
                unit_number: property.unit || property.unit_number || '',
                floor: property.floor || property.floor_number || null,
                maintenance_fee: property.maintenance || property.hoa_fee || property.common_charges || null
            };
            
            // Log mapped result for first few properties including image info
            if (index < 2) {
                console.log(`   ✅ Mapped property ${index + 1}:`, {
                    address: mappedProperty.address,
                    price: mappedProperty.price,
                    beds: mappedProperty.beds,
                    baths: mappedProperty.baths,
                    image_url: mappedProperty.image_url,
                    image_count: mappedProperty.image_count,
                    description: mappedProperty.description.substring(0, 100) + '...'
                });
            }
            
            return mappedProperty;
        }).filter(prop => prop.price > 0); // Only keep properties with valid prices

        console.log(`   ✅ Mapped to ${mappedProperties.length} valid properties`);
        
        // Log image extraction statistics
        const propertiesWithImages = mappedProperties.filter(p => p.image_url);
        const totalImages = mappedProperties.reduce((sum, p) => sum + (p.image_count || 0), 0);
        console.log(`   🖼️ Image extraction: ${propertiesWithImages.length}/${mappedProperties.length} properties with images`);
        console.log(`   📸 Total images found: ${totalImages}`);
        
        // Log sample of mapped data
        if (mappedProperties.length > 0) {
            const sample = mappedProperties[0];
            console.log(`   💰 Sample mapped: $${sample.price?.toLocaleString()}, ${sample.beds} beds, ${sample.baths} baths, ${sample.image_count} images`);
        }

        return mappedProperties;
    }

    /**
     * NEW: Comprehensive image URL extraction from StreetEasy API response
     */
    extractImageUrls(property) {
        const imageUrls = [];
        let primaryImageUrl = null;

        // Strategy 1: Try common StreetEasy image field patterns
        const imageFieldPatterns = [
            'image_url', 'image', 'photo_url', 'photo', 'primary_image',
            'images', 'photos', 'listing_images', 'property_images',
            'media', 'gallery', 'attachments', 'files'
        ];

        imageFieldPatterns.forEach(field => {
            if (property[field]) {
                if (Array.isArray(property[field])) {
                    // Handle array of images
                    property[field].forEach(img => {
                        const url = this.extractSingleImageUrl(img);
                        if (url && this.isValidImageUrl(url)) {
                            imageUrls.push(url);
                            if (!primaryImageUrl) primaryImageUrl = url;
                        }
                    });
                } else {
                    // Handle single image
                    const url = this.extractSingleImageUrl(property[field]);
                    if (url && this.isValidImageUrl(url)) {
                        imageUrls.push(url);
                        if (!primaryImageUrl) primaryImageUrl = url;
                    }
                }
            }
        });

        // Strategy 2: Look for nested image objects
        if (property.listing && property.listing.images) {
            this.processImageArray(property.listing.images, imageUrls, primaryImageUrl);
        }

        // Strategy 3: Look for media objects with different structures
        if (property.media) {
            if (Array.isArray(property.media)) {
                property.media.forEach(mediaItem => {
                    if (mediaItem.type === 'image' || mediaItem.media_type === 'photo') {
                        const url = this.extractSingleImageUrl(mediaItem);
                        if (url && this.isValidImageUrl(url)) {
                            imageUrls.push(url);
                            if (!primaryImageUrl) primaryImageUrl = url;
                        }
                    }
                });
            }
        }

        // Remove duplicates and clean URLs
        const uniqueUrls = [...new Set(imageUrls)]
            .map(url => this.cleanImageUrl(url))
            .filter(url => url && this.isValidImageUrl(url));

        return {
            primaryImageUrl: primaryImageUrl ? this.cleanImageUrl(primaryImageUrl) : null,
            allImageUrls: uniqueUrls,
            imageCount: uniqueUrls.length
        };
    }

    /**
     * Extract single image URL from various object formats
     */
    extractSingleImageUrl(imageObj) {
        if (typeof imageObj === 'string') {
            return imageObj;
        }
        
        if (typeof imageObj === 'object' && imageObj !== null) {
            // Try common URL field names
            return imageObj.url || imageObj.src || imageObj.href || 
                   imageObj.image_url || imageObj.photo_url ||
                   imageObj.large || imageObj.medium || imageObj.small ||
                   imageObj.original || imageObj.full || imageObj.thumb;
        }
        
        return null;
    }

    /**
     * Process array of image objects
     */
    processImageArray(images, imageUrls, primaryImageUrl) {
        if (!Array.isArray(images)) return;
        
        images.forEach(img => {
            const url = this.extractSingleImageUrl(img);
            if (url && this.isValidImageUrl(url)) {
                imageUrls.push(url);
                if (!primaryImageUrl) primaryImageUrl = url;
            }
        });
    }

    /**
     * Validate if URL is a valid image URL
     */
    isValidImageUrl(url) {
        if (!url || typeof url !== 'string') return false;
        
        // Check for valid image extensions
        const imageExtensions = /\.(jpg|jpeg|png|webp|gif|svg)(\?.*)?$/i;
        
        // Check for known image hosting domains
        const validDomains = [
            'photos.zillowstatic.com',
            'streeteasy-media.s3.amazonaws.com',
            'images.streeteasy.com',
            'photos.streeteasy.com',
            'media.streeteasy.com',
            'ssl.cdn-redfin.com',
            'ap.rdcpix.com'
        ];

        return imageExtensions.test(url) || validDomains.some(domain => url.includes(domain));
    }

    /**
     * Clean and normalize image URL
     */
    cleanImageUrl(url) {
        if (!url) return null;
        
        try {
            // Remove any extra parameters except essential ones
            const urlObj = new URL(url);
            
            // Keep some common image parameters but remove tracking
            const keepParams = ['w', 'h', 'q', 'format', 'auto', 'fit', 'crop'];
            const newSearchParams = new URLSearchParams();
            
            keepParams.forEach(param => {
                if (urlObj.searchParams.has(param)) {
                    newSearchParams.set(param, urlObj.searchParams.get(param));
                }
            });
            
            urlObj.search = newSearchParams.toString();
            
            // Ensure HTTPS
            urlObj.protocol = 'https:';
            
            return urlObj.toString();
        } catch (error) {
            // If URL parsing fails, just ensure HTTPS and return
            return url.replace(/^http:/, 'https:');
        }
    }

    /**
     * ENHANCED: Also with better debugging for past sales
     */
    async fetchPastSalesComparables(neighborhood) {
        try {
            console.log(`   📊 Fetching past sales for ${neighborhood}...`);
            
            const response = await axios.get(
                'https://streeteasy-api.p.rapidapi.com/sales/past/search',
                {
                    params: {
                        areas: neighborhood,
                        limit: 500,  // Get lots of comp data
                        // Add time filters for recent sales
                        minPrice: 200000,
                        maxPrice: 10000000,
                        offset: 0
                    },
                    headers: {
                        'X-RapidAPI-Key': this.rapidApiKey,
                        'X-RapidAPI-Host': 'streeteasy-api.p.rapidapi.com'
                    },
                    timeout: 30000
                }
            );

            console.log(`   📊 Past sales response keys:`, Object.keys(response.data || {}));
            
            let salesData = [];
            
            if (response.data) {
                // Log structure for debugging
                console.log(`   📋 Past sales structure:`, JSON.stringify(response.data, null, 2).substring(0, 300) + '...');
                
                if (response.data.results && Array.isArray(response.data.results)) {
                    salesData = response.data.results;
                } else if (response.data.listings && Array.isArray(response.data.listings)) {
                    salesData = response.data.listings;
                } else if (Array.isArray(response.data)) {
                    salesData = response.data;
                } else if (response.data.properties && Array.isArray(response.data.properties)) {
                    salesData = response.data.properties;
                } else if (response.data.sales && Array.isArray(response.data.sales)) {
                    salesData = response.data.sales;
                }
            }

            // Map to consistent format
            const mappedSales = salesData.map(sale => ({
                sale_price: sale.price || sale.sale_price || sale.sold_price || 0,
                beds: sale.beds || sale.bedrooms || sale.bed_count || 0,
                baths: sale.baths || sale.bathrooms || sale.bath_count || 0,
                property_type: sale.type || sale.property_type || sale.building_type || 'unknown',
                sale_date: sale.sale_date || sale.date_sold || sale.closed_date || sale.sold_date,
                address: sale.address || 'Address not available'
            })).filter(sale => sale.sale_price > 0); // Only valid sales

            console.log(`   ✅ Past sales: ${mappedSales.length} valid sales found`);
            
            if (mappedSales.length > 0) {
                const sample = mappedSales[0];
                console.log(`   💰 Sample past sale: $${sample.sale_price?.toLocaleString()}, ${sample.beds} beds`);
            }

            return mappedSales;

        } catch (error) {
            console.warn(`   ⚠️ Could not fetch past sales for ${neighborhood}: ${error.message}`);
            if (error.response) {
                console.warn(`   ⚠️ Past sales API error status: ${error.response.status}`);
                console.warn(`   ⚠️ Past sales API error data:`, JSON.stringify(error.response.data, null, 2).substring(0, 200));
            }
            return []; // Fallback to active listings comparison
        }
    }

    /**
     * ENHANCED APPROACH: Use past sales data for accurate comparables
     * Falls back to active listings if no past sales available
     */
    filterUndervaluedProperties(properties, neighborhood, pastSales = []) {
        const undervalued = [];

        console.log(`\n   🔍 DEBUGGING ${neighborhood.toUpperCase()}:`);
        console.log(`   📋 Active properties: ${properties.length}`);
        console.log(`   📊 Past sales: ${pastSales.length}`);

        // Show sample of what we received
        if (properties.length > 0) {
            const sample = properties[0];
            console.log(`   🏠 Sample property: $${sample.price?.toLocaleString() || 'NO PRICE'}, ${sample.beds || 'NO BEDS'} beds, ${sample.image_count || 0} images`);
        }

        // Prefer past sales data for comparables, fallback to active listings
        const comparableData = pastSales.length >= 3 ? pastSales : properties;
        const comparisonType = pastSales.length >= 3 ? 'recent sales' : 'active listings';
        
        console.log(`   🔍 Using ${comparableData.length} ${comparisonType} for comparison`);

        // Group comparables by bedroom count
        const comparablesByBeds = {};
        comparableData.forEach(comp => {
            const price = pastSales.length >= 3 ? comp.sale_price : comp.price;
            if (!price || price <= 0) return;
            
            const beds = comp.beds || '0';
            if (!comparablesByBeds[beds]) {
                comparablesByBeds[beds] = [];
            }
            comparablesByBeds[beds].push(price);
        });

        console.log(`   📊 Comparables by bedroom count:`, Object.keys(comparablesByBeds).map(beds => `${beds}-bed: ${comparablesByBeds[beds].length} properties`).join(', '));

        // Analyze each active listing
        for (const property of properties) {
            if (!property.price || property.price <= 0) continue;

            const beds = property.beds || '0';
            const comparablePrices = comparablesByBeds[beds] || [];
            
            // DEBUG: Log comparison details
            console.log(`   🏠 ${property.address}: $${property.price.toLocaleString()} (${beds} beds, ${property.image_count || 0} images)`);
            console.log(`       📊 Found ${comparablePrices.length} comparable ${beds}-bed ${comparisonType}`);
            
            // Need at least 3 comparable properties
            if (comparablePrices.length < 3) {
                console.log(`       ❌ Not enough comparables (need 3+)`);
                continue;
            }

            // Calculate market benchmarks with outlier removal
            const sortedPrices = [...comparablePrices].sort((a, b) => a - b);
            
            // Remove outliers (top and bottom 10%) for more accurate market price
            const trimmedPrices = sortedPrices.slice(
                Math.floor(sortedPrices.length * 0.1), 
                Math.ceil(sortedPrices.length * 0.9)
            );
            
            const medianPrice = trimmedPrices[Math.floor(trimmedPrices.length / 2)];
            const avgPrice = trimmedPrices.reduce((sum, price) => sum + price, 0) / trimmedPrices.length;
            
            // Use median as primary benchmark (more stable)
            const marketPrice = medianPrice;
            const discountPercent = ((marketPrice - property.price) / marketPrice) * 100;
            
            // DEBUG: Show calculation details
            console.log(`       💰 Market price: $${marketPrice.toLocaleString()} (from ${trimmedPrices.length} trimmed comparables)`);
            console.log(`       📉 Discount: ${discountPercent.toFixed(1)}% ${discountPercent >= 5 ? '✅ QUALIFIES' : '❌ Too small'}`);

            // Find properties 5%+ below comparable sales/listings
            if (discountPercent >= 5) {
                const distressSignals = this.findDistressSignals(property.description);
                const warningSignals = this.findWarningSignals(property.description);
                
                const scoreResult = this.calculateUndervaluationScore({
                    discountPercent,
                    distressSignals,
                    warningSignals,
                    neighborhood,
                    beds: property.beds,
                    baths: property.baths,
                    sqft: property.sqft,
                    comparableCount: comparablePrices.length,
                    comparisonType: comparisonType,
                    hasImages: property.image_count > 0
                });

                undervalued.push({
                    ...property,
                    market_price: marketPrice,
                    median_comparable_price: Math.round(medianPrice),
                    avg_comparable_price: Math.round(avgPrice),
                    discount_percent: Math.round(discountPercent * 10) / 10,
                    potential_savings: Math.round(marketPrice - property.price),
                    comparable_count: comparablePrices.length,
                    comparison_method: `${comparablePrices.length} comparable ${beds}-bed ${comparisonType} in ${neighborhood}`,
                    comparison_type: comparisonType, // 'recent sales' or 'active listings'
                    distress_signals: distressSignals,
                    warning_signals: warningSignals,
                    undervaluation_score: scoreResult.score,
                    deal_quality: scoreResult.dealQuality,
                    analysis_date: new Date().toISOString()
                });

                console.log(`       🎉 FOUND DEAL: ${scoreResult.dealQuality} (${discountPercent.toFixed(1)}% below market, ${property.image_count || 0} images)`);
            }
        }

        console.log(`   📈 Result: ${undervalued.length} undervalued properties found\n`);
        return undervalued;
    }

    /**
     * Get valid neighborhoods that exist in StreetEasy API
     */
    getValidNeighborhoods() {
        return HIGH_PRIORITY_NEIGHBORHOODS
            .map(n => n.toLowerCase().replace(/\s+/g, '-').replace(/[^a-z0-9-]/g, ''))
            .filter(slug => VALID_STREETEASY_SLUGS.has(slug))
            .slice(0, 20); // Limit to top 20 to keep runtime reasonable
    }

    findDistressSignals(description) {
        const distressKeywords = [
            'motivated seller', 'must sell', 'as-is', 'needs work', 'fixer-upper',
            'estate sale', 'inherited', 'price reduced', 'bring offers', 'cash only'
        ];
        const text = description.toLowerCase();
        return distressKeywords.filter(keyword => text.includes(keyword));
    }

    findWarningSignals(description) {
        const warningKeywords = [
            'flood damage', 'water damage', 'foundation issues', 'structural',
            'fire damage', 'mold', 'asbestos', 'lead paint', 'no permits'
        ];
        const text = description.toLowerCase();
        return warningKeywords.filter(keyword => text.includes(keyword));
    }

    calculateUndervaluationScore(factors) {
        let score = Math.min(factors.discountPercent * 1.5, 40); // Base score from discount
        
        // Enhanced scoring with deal quality labels
        let dealQuality = '';
        let qualityBonus = 0;
        
        if (factors.discountPercent >= 15) {
            dealQuality = 'UNICORN'; // 15%+ below market = Unicorn (very rare)
            qualityBonus = 20;
        } else if (factors.discountPercent >= 10) {
            dealQuality = 'EXCELLENT'; // 10%+ below market = Excellent deal
            qualityBonus = 15;
        } else if (factors.discountPercent >= 5) {
            dealQuality = 'GOOD'; // 5-8% below market = Good deal
            qualityBonus = 10;
        } else {
            dealQuality = 'FAIR'; // Less than 5% below = Fair deal
            qualityBonus = 5;
        }
        
        score += qualityBonus;
        score += Math.min(factors.distressSignals.length * 3, 15);
        
        // Property characteristics bonus
        if (factors.beds >= 3) score += 8;
        else if (factors.beds >= 2) score += 5;
        else score += 2;
        
        if (factors.baths >= 2) score += 5;
        else if (factors.baths >= 1) score += 3;
        
        if (factors.sqft >= 1000) score += 8;
        else if (factors.sqft >= 700) score += 5;
        
        // NEW: Image bonus - properties with images are more valuable
        if (factors.hasImages) score += 5;
        
        // Comparable data quality bonus
        if (factors.comparableCount >= 10) score += 10;
        else if (factors.comparableCount >= 5) score += 7;
        else if (factors.comparableCount >= 3) score += 3;
        
        // Bonus for using actual sales data vs active listings
        if (factors.comparisonType === 'recent sales') score += 8;
        
        score -= Math.min(factors.warningSignals.length * 3, 10);
        
        return {
            score: Math.max(0, Math.min(100, Math.round(score))), // Cap at 100
            dealQuality: dealQuality
        };
    }

    /**
     * ENHANCED: Save to database with image data and return detailed stats
     */
    async saveUndervaluedPropertiesWithStats(properties, neighborhood) {
        if (!properties || properties.length === 0) {
            return { newCount: 0, updateCount: 0, duplicateCount: 0 };
        }

        console.log(`   💾 Checking ${properties.length} properties for duplicates...`);
        
        let newCount = 0;
        let duplicateCount = 0;
        let updateCount = 0;

        try {
            for (const property of properties) {
                // Check for existing property by listing_id or address+price combination
                const { data: existing, error: checkError } = await this.supabase
                    .from('undervalued_properties')
                    .select('id, undervaluation_score, analysis_date')
                    .or(`listing_id.eq.${property.listing_id},and(address.eq."${property.address}",price.eq.${property.price})`)
                    .single();

                if (checkError && checkError.code !== 'PGRST116') {
                    // PGRST116 = no rows found (not an error)
                    console.error(`   ❌ Error checking for existing property:`, checkError.message);
                    continue;
                }

                // ENHANCED: Database record with image data
                const dbRecord = {
                    listing_id: property.listing_id,
                    address: property.address,
                    neighborhood: property.neighborhood,
                    price: property.price,
                    sqft: property.sqft || null,
                    beds: property.beds || 0,
                    baths: property.baths || 0,
                    description: (property.description || '').substring(0, 1000),
                    url: property.url,
                    property_type: property.property_type,
                    
                    // NEW: Image fields
                    image_url: property.image_url,
                    image_urls: property.image_urls || [],
                    image_count: property.image_count || 0,
                    primary_image_verified: false, // Will be verified in background
                    
                    // Enhanced comparison fields
                    market_price: property.market_price,
                    comparable_count: property.comparable_count,
                    comparison_method: property.comparison_method,
                    deal_quality: property.deal_quality,
                    median_comparable_price: property.median_comparable_price,
                    avg_comparable_price: property.avg_comparable_price,
                    comparison_type: property.comparison_type,
                    
                    // Analysis fields
                    discount_percent: property.discount_percent,
                    potential_savings: property.potential_savings,
                    distress_signals: property.distress_signals || [],
                    warning_signals: property.warning_signals || [],
                    undervaluation_score: property.undervaluation_score,
                    
                    // Additional property details
                    days_on_market: property.days_on_market || null,
                    building_name: property.building_name || null,
                    unit_number: property.unit_number || null,
                    floor: property.floor || null,
                    maintenance_fee: property.maintenance_fee || null,
                    
                    analysis_date: property.analysis_date,
                    status: 'active'
                };

                if (existing) {
                    // Property exists - decide whether to update
                    const existingScore = existing.undervaluation_score || 0;
                    const newScore = property.undervaluation_score || 0;
                    
                    // Update if score improved significantly (5+ points) or data is much newer
                    const scoreImproved = newScore > (existingScore + 5);
                    const isNewerData = new Date(property.analysis_date) > new Date(existing.analysis_date);
                    
                    if (scoreImproved || (isNewerData && newScore >= existingScore)) {
                        // Update existing record
                        const { error: updateError } = await this.supabase
                            .from('undervalued_properties')
                            .update(dbRecord)
                            .eq('id', existing.id);

                        if (updateError) {
                            console.error(`   ❌ Error updating ${property.address}:`, updateError.message);
                        } else {
                            console.log(`   🔄 Updated: ${property.address} (${property.image_count || 0} images, score: ${existingScore} → ${newScore})`);
                            updateCount++;
                        }
                    } else {
                        console.log(`   ⏭️ Skipped duplicate: ${property.address} (score: ${newScore}, existing: ${existingScore})`);
                        duplicateCount++;
                    }
                } else {
                    // New property - insert it
                    const { error: insertError } = await this.supabase
                        .from('undervalued_properties')
                        .insert([dbRecord]);

                    if (insertError) {
                        console.error(`   ❌ Error inserting ${property.address}:`, insertError.message);
                    } else {
                        console.log(`   ✅ Added: ${property.address} (${property.image_count || 0} images, ${property.discount_percent}% below ${property.comparison_type}, ${property.deal_quality}, score: ${property.undervaluation_score})`);
                        newCount++;
                    }
                }

                // Small delay to avoid overwhelming database
                await this.delay(100);
            }

            return { newCount, updateCount, duplicateCount };

        } catch (error) {
            console.error(`❌ Save error for ${neighborhood}:`, error.message);
            return { newCount: 0, updateCount: 0, duplicateCount: 0 };
        }
    }

    async clearOldUndervaluedProperties() {
        try {
            const oneWeekAgo = new Date();
            oneWeekAgo.setDate(oneWeekAgo.getDate() - 7);

            const { error } = await this.supabase
                .from('undervalued_properties')
                .delete()
                .lt('analysis_date', oneWeekAgo.toISOString());

            if (error) {
                console.error('❌ Error clearing old properties:', error.message);
            } else {
                console.log(`🧹 Cleared old properties from database`);
            }
        } catch (error) {
            console.error('❌ Clear old properties error:', error.message);
        }
    }

    async saveWeeklySummary(summary) {
        try {
            const { error } = await this.supabase
                .from('weekly_analysis_runs')
                .insert([{
                    run_date: summary.startTime,
                    neighborhoods_checked: summary.neighborhoodsProcessed,
                    total_properties_fetched: summary.totalPropertiesFetched,
                    undervalued_found: summary.undervaluedFound,
                    saved_to_database: summary.savedToDatabase,
                    updated_in_database: summary.updatedInDatabase,
                    duplicates_skipped: summary.duplicatesSkipped,
                    api_calls_used: summary.apiCallsUsed,
                    duration_minutes: Math.round(summary.duration),
                    errors: summary.errors,
                    
                    // NEW: Image processing stats
                    properties_with_images: summary.propertiesWithImages,
                    images_verified: 0, // Will be updated by background verification
                    image_verification_errors: 0,
                    
                    completed: true
                }]);

            if (error) {
                console.error('❌ Error saving weekly summary:', error.message);
            } else {
                console.log('✅ Weekly summary saved to database');
            }
        } catch (error) {
            console.error('❌ Save summary error:', error.message);
        }
    }

    logWeeklySummary(summary) {
        console.log('\n📊 ENHANCED WEEKLY ANALYSIS COMPLETE');
        console.log('='.repeat(60));
        console.log(`⏱️ Duration: ${summary.duration.toFixed(1)} minutes`);
        console.log(`🗽 Neighborhoods processed: ${summary.neighborhoodsProcessed}`);
        console.log(`📡 Total properties fetched: ${summary.totalPropertiesFetched}`);
        console.log(`🖼️ Properties with images: ${summary.propertiesWithImages}`);
        console.log(`📸 Total images extracted: ${summary.totalImagesExtracted}`);
        console.log(`🎯 Undervalued properties found: ${summary.undervaluedFound}`);
        console.log(`💾 New properties saved: ${summary.savedToDatabase}`);
        console.log(`🔄 Properties updated: ${summary.updatedInDatabase}`);
        console.log(`⏭️ Duplicates skipped: ${summary.duplicatesSkipped}`);
        console.log(`📞 API calls used: ${summary.apiCallsUsed}`);
        console.log(`⚡ Rate limit hits: ${summary.rateLimitHits}`);
        console.log(`⏰ Final delay setting: ${this.baseDelay/1000}s between calls`);
        
        // Image extraction statistics
        const imageSuccessRate = summary.totalPropertiesFetched > 0 ? 
            (summary.propertiesWithImages / summary.totalPropertiesFetched * 100).toFixed(1) : '0';
        console.log(`🖼️ Image extraction rate: ${imageSuccessRate}% of properties have images`);
        
        if (summary.errors.length > 0) {
            const rateLimitErrors = summary.errors.filter(e => e.isRateLimit).length;
            const notFoundErrors = summary.errors.filter(e => e.is404).length;
            const otherErrors = summary.errors.length - rateLimitErrors - notFoundErrors;
            console.log(`❌ Errors: ${summary.errors.length} total (${rateLimitErrors} rate limits, ${notFoundErrors} 404s, ${otherErrors} other)`);
            
            if (notFoundErrors > 0) {
                console.log(`🔧 404 Errors suggest API endpoint/parameter issues - check StreetEasy API docs`);
            }
        }

        if (summary.savedToDatabase > 0) {
            console.log('\n🎉 SUCCESS: Found and saved undervalued properties with images!');
            console.log(`🖼️ Direct image display enabled - Zero bandwidth cost!`);
        } else {
            console.log('\n📊 No undervalued properties found (normal in competitive NYC market)');
        }
        
        const successRate = summary.neighborhoodsProcessed > 0 ? 
            ((summary.neighborhoodsProcessed - summary.errors.filter(e => e.is404).length) / summary.apiCallsUsed * 100).toFixed(1) : '0';
        console.log(`📈 Success rate: ${successRate}% of API calls succeeded (excluding 404s)`);
        
        const totalDbOperations = summary.savedToDatabase + summary.updatedInDatabase + summary.duplicatesSkipped;
        if (totalDbOperations > 0) {
            console.log(`🔒 Duplicate prevention: ${summary.duplicatesSkipped}/${totalDbOperations} (${(summary.duplicatesSkipped/totalDbOperations*100).toFixed(1)}%) duplicates prevented`);
        }
    }

    delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

// Main execution
async function runWeeklyAnalysis() {
    if (!process.env.RAPIDAPI_KEY || !process.env.SUPABASE_URL || !process.env.SUPABASE_ANON_KEY) {
        console.error('❌ Missing required environment variables!');
        console.error('   RAPIDAPI_KEY, SUPABASE_URL, SUPABASE_ANON_KEY required');
        process.exit(1);
    }

    const analyzer = new OptimalWeeklyStreetEasy();
    
    try {
        console.log('🗽 Starting ENHANCED Weekly StreetEasy Analysis...\n');
        console.log('🔧 Enhanced features:');
        console.log('   - Fixed endpoint: /sales/search with areas parameter');
        console.log('   - Comprehensive image extraction from API responses');
        console.log('   - Direct image display (zero bandwidth cost)');
        console.log('   - Enhanced property scoring with image bonuses');
        console.log('   - Improved database schema with image fields');
        console.log('   - Duplicate prevention and update logic\n');
        
        const results = await analyzer.runWeeklyUndervaluedRefresh();
        
        console.log('\n✅ Enhanced analysis completed!');
        
        if (results.errors.filter(e => e.is404).length > 0) {
            console.log('\n⚠️ If you still get 404 errors, double-check:');
            console.log('   1. StreetEasy API documentation for exact endpoints');
            console.log('   2. Your RapidAPI subscription is active');
            console.log('   3. Neighborhood names match API requirements');
        }
        
        console.log(`📊 Check your Supabase 'undervalued_properties' table for ${results.savedToDatabase} new deals with images!`);
        console.log(`🖼️ ${results.propertiesWithImages} properties now have direct image display capability!`);
        
        return results;
        
    } catch (error) {
        console.error('💥 Enhanced analysis failed:', error.message);
        process.exit(1);
    }
}

// Export for use in scheduler
module.exports = OptimalWeeklyStreetEasy;

// Run if executed directly
if (require.main === module) {
    runWeeklyAnalysis().catch(console.error);
}
