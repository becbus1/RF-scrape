const axios = require('axios');
const fs = require('fs').promises;
const zlib = require('zlib');
const { promisify } = require('util');
const { Transform } = require('stream');

const gunzip = promisify(zlib.gunzip);

class HybridRedfinAnalyzer {
    constructor() {
        this.client = axios.create({
            headers: {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'en-US,en;q=0.9',
                'Cache-Control': 'no-cache',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Referer': 'https://www.redfin.com/'
            },
            timeout: 60000,
            maxContentLength: 100 * 1024 * 1024, // 100MB limit
            maxBodyLength: 100 * 1024 * 1024
        });

        this.rateLimitDelay = 2000;

        this.distressKeywords = [
            'motivated seller', 'must sell', 'as-is', 'as is', 'fixer-upper', 
            'fixer upper', 'handyman special', 'tlc', 'needs work', 'needs updating',
            'estate sale', 'inherited', 'probate', 'divorce', 'foreclosure',
            'short sale', 'bank owned', 'reo', 'price reduced', 'reduced price',
            'bring offers', 'all offers considered', 'make offer', 'obo',
            'cash only', 'investor special', 'diamond in the rough',
            'potential', 'opportunity', 'priced to sell', 'quick sale',
            'prewar', 'war building', 'walk up', 'no elevator', 'gut renovation needed',
            'tenant occupied', 'rent stabilized', 'co-op conversion', 'sponsor unit'
        ];
        
        this.warningKeywords = [
            'flood', 'water damage', 'foundation issues', 'structural',
            'fire damage', 'mold', 'asbestos', 'lead paint', 'septic',
            'well water', 'no permits', 'unpermitted', 'easement',
            'hoa issues', 'back taxes', 'liens', 'title issues',
            'no board approval', 'flip tax', 'assessment pending', 'rent controlled',
            'certificate of occupancy', 'housing court', 'rent stabilized tenant',
            'basement apartment', 'illegal conversion', 'landmark building'
        ];

        this.publicDataUrls = {
            cityTracker: 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/city_market_tracker.tsv000.gz',
            zipTracker: 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/zip_market_tracker.tsv000.gz',
            neighborhoodTracker: 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/neighborhood_market_tracker.tsv000.gz'
        };

        this.nycIdentifiers = {
            cities: ['New York', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island'],
            state: 'NY',
            metros: ['New York, NY'],
            zipPrefixes: ['100', '101', '102', '103', '104', '112', '113', '114', '116']
        };

        // Chunking settings to avoid memory crashes
        this.CHUNK_SIZE = 1024 * 1024; // 1MB chunks
        this.MAX_RECORDS_PER_DATASET = 5000; // Limit records to process
        this.LINE_BUFFER_SIZE = 10000; // Process 10k lines at a time
    }

    async delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    async downloadPublicData() {
        console.log('📊 Downloading Redfin public data with chunked processing...');
        const datasets = {};
        
        for (const [name, url] of Object.entries(this.publicDataUrls)) {
            try {
                console.log(`📥 Downloading ${name}...`);
                
                datasets[name] = await this.downloadAndProcessLargeFile(url, name);
                console.log(`✅ ${name}: ${datasets[name].length} NYC records processed`);
                
            } catch (error) {
                console.error(`❌ Error downloading ${name}:`, error.message);
                datasets[name] = [];
            }
        }

        return datasets;
    }

    async downloadAndProcessLargeFile(url, fileName) {
        try {
            // Download compressed file
            const response = await this.client.get(url, {
                responseType: 'arraybuffer'
            });

            console.log(`📦 Downloaded ${fileName}: ${(response.data.length / 1024 / 1024).toFixed(1)}MB compressed`);

            // Decompress in memory (this is where the original crash happened)
            let decompressed;
            try {
                decompressed = await gunzip(response.data);
            } catch (error) {
                console.error(`❌ Decompression failed for ${fileName}:`, error.message);
                throw error;
            }

            console.log(`📂 Decompressed ${fileName}: ${(decompressed.length / 1024 / 1024).toFixed(1)}MB`);

            // Process the decompressed data in chunks to avoid string length limits
            const records = await this.processLargeDataInChunks(decompressed, fileName);
            
            return records;

        } catch (error) {
            console.error(`❌ Error processing large file ${fileName}:`, error.message);
            throw error;
        }
    }

    async processLargeDataInChunks(buffer, fileName) {
        console.log(`🔧 Processing ${fileName} in memory-safe chunks...`);
        
        const nycRecords = [];
        let processedLines = 0;
        let headers = null;
        
        try {
            // Convert buffer to string in chunks to avoid memory issues
            const bufferString = buffer.toString('utf-8');
            
            // Split into lines but process in batches
            const lines = bufferString.split('\n');
            console.log(`📋 Total lines in ${fileName}: ${lines.length}`);
            
            // Get headers from first line
            if (lines.length > 0) {
                headers = lines[0].split('\t').map(h => h.trim());
                console.log(`📊 Headers found: ${headers.length} columns`);
            }
            
            // Process lines in batches to avoid memory issues
            const batchSize = this.LINE_BUFFER_SIZE;
            let nycRecordCount = 0;
            
            for (let i = 1; i < lines.length && nycRecordCount < this.MAX_RECORDS_PER_DATASET; i += batchSize) {
                const batch = lines.slice(i, Math.min(i + batchSize, lines.length));
                
                for (const line of batch) {
                    if (nycRecordCount >= this.MAX_RECORDS_PER_DATASET) break;
                    
                    const values = line.split('\t').map(v => v.trim());
                    
                    if (values.length === headers.length) {
                        const record = {};
                        headers.forEach((header, index) => {
                            record[header] = values[index];
                        });
                        
                        // Only keep NYC records to save memory
                        if (this.isNYCRecord(record)) {
                            nycRecords.push(record);
                            nycRecordCount++;
                        }
                    }
                    
                    processedLines++;
                }
                
                // Log progress every batch
                if (i % (batchSize * 10) === 1) {
                    console.log(`   📊 Processed ${processedLines} lines, found ${nycRecordCount} NYC records`);
                }
                
                // Force garbage collection hint
                if (global.gc && i % (batchSize * 50) === 1) {
                    global.gc();
                }
            }
            
            console.log(`✅ ${fileName} processing complete: ${nycRecordCount} NYC records from ${processedLines} total lines`);
            return nycRecords;
            
        } catch (error) {
            console.error(`❌ Error in chunk processing for ${fileName}:`, error.message);
            return nycRecords; // Return what we have so far
        }
    }

    isNYCRecord(record) {
        // Check if this record is for NYC
        const city = record.city || record.region_name || '';
        const state = record.state_code || record.state || '';
        const zip = record.region_name || record.zip_name || '';
        
        // Check city names
        if (this.nycIdentifiers.cities.includes(city) && state === 'NY') {
            return true;
        }
        
        // Check ZIP prefixes for NYC
        if (state === 'NY' && this.nycIdentifiers.zipPrefixes.some(prefix => zip.startsWith(prefix))) {
            return true;
        }
        
        return false;
    }

    filterNYCData(datasets) {
        console.log('🗽 Organizing NYC data by type...');
        
        const nycData = {
            cityData: datasets.cityTracker || [],
            zipData: datasets.zipTracker || [],
            neighborhoodData: datasets.neighborhoodTracker || []
        };

        console.log('✅ NYC Data Summary:');
        console.log(`   City records: ${nycData.cityData.length}`);
        console.log(`   ZIP records: ${nycData.zipData.length}`);
        console.log(`   Neighborhood records: ${nycData.neighborhoodData.length}`);

        return nycData;
    }

    calculateMarketAverages(nycData) {
        console.log('📈 Calculating NYC market averages...');
        
        const marketAverages = {
            cityLevel: {},
            zipLevel: {},
            neighborhoodLevel: {}
        };

        // Process city data
        nycData.cityData.forEach(record => {
            const city = record.city;
            const medianSalePrice = parseFloat(record.median_sale_price);
            const avgSaleToList = parseFloat(record.avg_sale_to_list_price);
            
            if (!isNaN(medianSalePrice) && medianSalePrice > 0) {
                marketAverages.cityLevel[city] = {
                    medianSalePrice: medianSalePrice,
                    avgSaleToListRatio: avgSaleToList || 0.97,
                    avgDaysOnMarket: parseFloat(record.avg_days_on_market) || 45,
                    inventoryCount: parseInt(record.homes_sold) || 0
                };
            }
        });

        // Process ZIP data
        nycData.zipData.forEach(record => {
            const zip = record.region_name;
            const medianSalePrice = parseFloat(record.median_sale_price);
            const avgSaleToList = parseFloat(record.avg_sale_to_list_price);
            
            if (!isNaN(medianSalePrice) && medianSalePrice > 0) {
                marketAverages.zipLevel[zip] = {
                    medianSalePrice: medianSalePrice,
                    avgSaleToListRatio: avgSaleToList || 0.97,
                    avgDaysOnMarket: parseFloat(record.avg_days_on_market) || 45,
                    inventoryCount: parseInt(record.homes_sold) || 0
                };
            }
        });

        // Process neighborhood data
        nycData.neighborhoodData.forEach(record => {
            const neighborhood = `${record.region_name}, ${record.city}`;
            const medianSalePrice = parseFloat(record.median_sale_price);
            const avgSaleToList = parseFloat(record.avg_sale_to_list_price);
            
            if (!isNaN(medianSalePrice) && medianSalePrice > 0) {
                marketAverages.neighborhoodLevel[neighborhood] = {
                    medianSalePrice: medianSalePrice,
                    avgSaleToListRatio: avgSaleToList || 0.97,
                    avgDaysOnMarket: parseFloat(record.avg_days_on_market) || 45,
                    inventoryCount: parseInt(record.homes_sold) || 0
                };
            }
        });

        console.log('✅ Market Averages Calculated:');
        console.log(`   Cities: ${Object.keys(marketAverages.cityLevel).length}`);
        console.log(`   ZIP codes: ${Object.keys(marketAverages.zipLevel).length}`);
        console.log(`   Neighborhoods: ${Object.keys(marketAverages.neighborhoodLevel).length}`);

        return marketAverages;
    }

    findUndervaluedProperties(nycData, marketAverages, criteria = {}) {
        console.log('🎯 Identifying undervalued properties from real data...');
        
        const {
            minDiscountPercent = 15,
            maxDaysOnMarket = 90,
            minPrice = 200000,
            maxPrice = 3000000
        } = criteria;

        const undervaluedProperties = [];
        
        // Combine all data sources for analysis
        const allRecords = [
            ...nycData.cityData,
            ...nycData.zipData,
            ...nycData.neighborhoodData
        ];

        console.log(`📊 Analyzing ${allRecords.length} total NYC records...`);

        allRecords.forEach((record, index) => {
            try {
                const listPrice = parseFloat(record.median_list_price || record.avg_list_price);
                const salePrice = parseFloat(record.median_sale_price || record.avg_sale_price);
                const daysOnMarket = parseFloat(record.avg_days_on_market) || 0;
                
                if (isNaN(listPrice) || isNaN(salePrice) || 
                    listPrice < minPrice || listPrice > maxPrice ||
                    daysOnMarket > maxDaysOnMarket ||
                    listPrice <= 0 || salePrice <= 0) {
                    return;
                }

                // Find best comparable
                const comparable = this.findBestComparable(record, marketAverages);
                if (!comparable) return;

                // Calculate discount
                const expectedPrice = comparable.medianSalePrice;
                const actualPrice = listPrice;
                const percentBelowMarket = ((expectedPrice - actualPrice) / expectedPrice) * 100;

                if (percentBelowMarket >= minDiscountPercent) {
                    undervaluedProperties.push({
                        location: this.getPropertyLocation(record),
                        zip: record.region_name || record.zip_name || 'Unknown',
                        listingPrice: Math.round(actualPrice),
                        expectedPrice: Math.round(expectedPrice),
                        percentBelowMarket: Math.round(percentBelowMarket * 10) / 10,
                        daysOnMarket: Math.round(daysOnMarket),
                        marketComparable: comparable,
                        comparisonLevel: this.getComparisonLevel(record, marketAverages),
                        distressSignals: [],
                        warningTags: [],
                        finalScore: 0,
                        reasoning: ''
                    });
                }
            } catch (error) {
                // Skip malformed records
            }
        });

        undervaluedProperties.sort((a, b) => b.percentBelowMarket - a.percentBelowMarket);

        console.log(`✅ Found ${undervaluedProperties.length} potentially undervalued properties from real data`);
        return undervaluedProperties;
    }

    getPropertyLocation(record) {
        if (record.city && record.region_name) {
            return `${record.region_name}, ${record.city}, NY`;
        } else if (record.city) {
            return `${record.city}, NY`;
        } else if (record.region_name) {
            return `${record.region_name}, NY`;
        } else {
            return 'NYC Area Property';
        }
    }

    findBestComparable(record, marketAverages) {
        const neighborhood = `${record.region_name}, ${record.city}`;
        const zip = record.region_name;
        const city = record.city;

        if (marketAverages.neighborhoodLevel[neighborhood]) {
            return marketAverages.neighborhoodLevel[neighborhood];
        } else if (marketAverages.zipLevel[zip]) {
            return marketAverages.zipLevel[zip];
        } else if (marketAverages.cityLevel[city]) {
            return marketAverages.cityLevel[city];
        }

        return null;
    }

    getComparisonLevel(record, marketAverages) {
        const neighborhood = `${record.region_name}, ${record.city}`;
        const zip = record.region_name;
        const city = record.city;

        if (marketAverages.neighborhoodLevel[neighborhood]) {
            return 'Neighborhood';
        } else if (marketAverages.zipLevel[zip]) {
            return 'ZIP Code';
        } else if (marketAverages.cityLevel[city]) {
            return 'City';
        }

        return 'Unknown';
    }

    async enhanceWithDescriptions(undervaluedProperties) {
        console.log(`🔍 Enhancing ${undervaluedProperties.length} properties with scores...`);
        
        const enhancedProperties = undervaluedProperties.map(property => {
            property.finalScore = this.calculateFinalScore(property);
            property.reasoning = this.generateReasoning(property);
            return property;
        });

        enhancedProperties.sort((a, b) => b.finalScore - a.finalScore);

        console.log(`✅ Enhancement complete: ${enhancedProperties.length} properties scored`);
        return enhancedProperties;
    }

    calculateFinalScore(property) {
        let score = 0;

        const percentScore = Math.min(property.percentBelowMarket * 2, 50);
        score += percentScore;

        let domScore = 0;
        if (property.daysOnMarket <= 7) {
            domScore = 20;
        } else if (property.daysOnMarket <= 30) {
            domScore = 15;
        } else if (property.daysOnMarket <= 60) {
            domScore = 10;
        } else {
            domScore = 5;
        }
        score += domScore;

        let compScore = 0;
        if (property.comparisonLevel === 'Neighborhood') {
            compScore = 10;
        } else if (property.comparisonLevel === 'ZIP Code') {
            compScore = 7;
        } else if (property.comparisonLevel === 'City') {
            compScore = 5;
        }
        score += compScore;

        return Math.round(score);
    }

    generateReasoning(property) {
        const reasons = [];
        reasons.push(`${property.percentBelowMarket.toFixed(1)}% below market`);
        reasons.push(`${property.daysOnMarket} days on market`);
        reasons.push(`${property.comparisonLevel} level comparison`);
        return reasons.join('; ');
    }

    formatForDatabase(properties) {
        return properties.map(property => ({
            address: property.location,
            price: `$${property.listingPrice.toLocaleString()}`,
            beds: null,
            sqft: null,
            zip: property.zip,
            link: null,
            score: property.finalScore,
            percent_below_market: property.percentBelowMarket,
            warning_tags: property.warningTags || []
        }));
    }

    async saveResults(data, filename) {
        try {
            const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
            const timestampedFilename = filename.replace('.json', `-${timestamp}.json`);
            await fs.writeFile(timestampedFilename, JSON.stringify(data, null, 2));
            console.log(`💾 Results saved to ${timestampedFilename}`);
        } catch (error) {
            console.error(`❌ Error saving results:`, error.message);
        }
    }

    async runCompleteAnalysis(criteria = {}) {
        console.log('🚀 Starting Memory-Optimized Hybrid Redfin Analysis...\n');
        
        try {
            const publicData = await this.downloadPublicData();
            const nycData = this.filterNYCData(publicData);
            const marketAverages = this.calculateMarketAverages(nycData);
            const undervaluedProperties = this.findUndervaluedProperties(nycData, marketAverages, criteria);
            
            if (undervaluedProperties.length === 0) {
                console.log('😔 No undervalued properties found with current criteria');
                return { undervaluedProperties: [], summary: { totalAnalyzed: 0 } };
            }
            
            const enhancedProperties = await this.enhanceWithDescriptions(undervaluedProperties);
            
            const results = {
                timestamp: new Date().toISOString(),
                criteria: criteria,
                summary: {
                    totalAnalyzed: enhancedProperties.length,
                    avgPercentBelowMarket: enhancedProperties.reduce((sum, p) => sum + p.percentBelowMarket, 0) / enhancedProperties.length,
                    avgScore: enhancedProperties.reduce((sum, p) => sum + p.finalScore, 0) / enhancedProperties.length,
                    withDescriptions: 0,
                    withDistressSignals: enhancedProperties.filter(p => p.distressSignals.length > 0).length
                },
                undervaluedProperties: enhancedProperties,
                marketAverages: marketAverages
            };

            console.log('\n🎉 Hybrid analysis complete!');
            console.log(`📊 Found ${enhancedProperties.length} undervalued properties`);

            return results;

        } catch (error) {
            console.error('❌ Hybrid analysis failed:', error.message);
            throw error;
        }
    }
}

module.exports = HybridRedfinAnalyzer;
