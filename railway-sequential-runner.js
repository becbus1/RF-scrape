#!/usr/bin/env node

/**
 * CLAUDE-INTEGRATED RAILWAY RUNNER - COMPLETE VERSION
 * 
 * Combines the robust original structure with Claude integration:
 * 1. Auto-downloads DHCR files 
 * 2. Creates necessary directories
 * 3. Runs BOTH Claude systems (sales + rentals)
 * 4. Proper error handling and fallbacks
 */

require('dotenv').config();
const fs = require('fs').promises;
const path = require('path');
const axios = require('axios');

// Import BOTH Claude-powered systems
const EnhancedBiWeeklySalesAnalyzer = require('./biweekly-streeteasy-sales');
const ClaudePoweredRentalsSystem = require('./claude-powered-rentals-system');

class ClaudeIntegratedRailwayRunner {
    constructor() {
        this.startTime = Date.now();
        this.results = {
            dhcrStatus: { downloaded: 0, parsed: 0, errors: [] },
            salesResults: null,
            rentalsResults: null,
            totalDuration: 0
        };
    }

    /**
     * Main entry point - runs both Claude systems
     */
    async runBothClaudeSystems() {
        console.log('🚀 CLAUDE-INTEGRATED RAILWAY DEPLOYMENT');
        console.log('=' .repeat(60));
        
        try {
            // Step 1: Create necessary directories
            await this.createRequiredDirectories();
            
            // Step 2: Handle DHCR files (for rentals system)
            await this.ensureDHCRFiles();
            
            // Step 3: Check environment variables
            await this.checkEnvironmentVariables();
            
            // Step 4: Determine which systems to run
            const runSalesOnly = process.env.RUN_SALES_ONLY === 'true';
            const runRentalsOnly = process.env.RUN_RENTALS_ONLY === 'true';
            
            if (runSalesOnly) {
                console.log('🏠 RUNNING SALES ONLY (RUN_SALES_ONLY=true)\n');
                return await this.runSalesOnly();
            }
            
            if (runRentalsOnly) {
                console.log('🏘️ RUNNING RENTALS ONLY (RUN_RENTALS_ONLY=true)\n');
                return await this.runRentalsOnly();
            }
            
            // Step 5: Run both systems with proper spacing
            console.log('🎯 Running BOTH Claude systems with rate limit protection...\n');
            
            // SYSTEM 1: Claude-integrated sales scraper
            console.log('🏠 [1/2] CLAUDE SALES ANALYSIS STARTING...');
            const salesResults = await this.runSalesAnalysis();
            this.results.salesResults = salesResults;
            console.log('✅ Sales analysis complete\n');
            
            // Rate limit protection between systems
            console.log('⏰ Waiting 5 minutes before rentals (rate limit protection)...');
            await this.delay(5 * 60 * 1000); // 5 minutes
            
            // SYSTEM 2: Claude-powered rentals system
            console.log('🏘️ [2/2] CLAUDE RENTALS ANALYSIS STARTING...');
            const rentalsResults = await this.runRentalsAnalysis();
            this.results.rentalsResults = rentalsResults;
            console.log('✅ Rentals analysis complete\n');
            
            // Step 6: Log comprehensive results
            this.logCombinedResults();
            
            return {
                sales: salesResults,
                rentals: rentalsResults
            };
            
        } catch (error) {
            console.error('💥 CLAUDE RAILWAY DEPLOYMENT FAILED:', error.message);
            console.error('\n🔧 DEBUG INFO:');
            console.error('   - Error type:', error.constructor.name);
            console.error('   - Stack trace:', error.stack?.split('\n')[1]);
            console.error('\n📋 TROUBLESHOOTING CHECKLIST:');
            console.error('   ✅ Environment variables set?');
            console.error('   ✅ Supabase connection working?');
            console.error('   ✅ ANTHROPIC_API_KEY configured?');
            console.error('   ✅ Both Claude system files exist?');
            console.error('   ✅ DHCR files downloaded or available?');
            
            throw error;
        }
    }

    /**
     * Run only sales analysis
     */
    async runSalesOnly() {
        const salesResults = await this.runSalesAnalysis();
        this.results.salesResults = salesResults;
        this.logSalesOnlyResults();
        return { sales: salesResults };
    }

    /**
     * Run only rentals analysis
     */
    async runRentalsOnly() {
        const rentalsResults = await this.runRentalsAnalysis();
        this.results.rentalsResults = rentalsResults;
        this.logRentalsOnlyResults();
        return { rentals: rentalsResults };
    }

    /**
     * Run Claude-integrated sales analysis
     */
    async runSalesAnalysis() {
        try {
            const salesAnalyzer = new EnhancedBiWeeklySalesAnalyzer();
            return await salesAnalyzer.runBiWeeklySalesRefresh();
        } catch (error) {
            console.error('❌ Sales analysis failed:', error.message);
            return { error: error.message };
        }
    }

    /**
     * Run Claude-powered rentals analysis
     */
    async runRentalsAnalysis() {
        try {
            const rentalsSystem = new ClaudePoweredRentalsSystem();
            
            // Setup database if needed
            await this.setupDatabaseIfNeeded(rentalsSystem);
            
            // Determine target neighborhoods with fallbacks
            const neighborhoods = await this.determineTargetNeighborhoods();
            
            // Configure analysis
            const analysisConfig = {
                neighborhoods: neighborhoods,
                maxListingsPerNeighborhood: parseInt(process.env.MAX_LISTINGS_PER_NEIGHBORHOOD) || 500,
                testMode: process.env.TEST_NEIGHBORHOOD ? true : false
            };
            
            console.log(`📍 Targeting ${neighborhoods.length} neighborhoods for rentals analysis`);
            
            // Run the analysis
            return await rentalsSystem.findUndervaluedRentStabilizedListings(analysisConfig);
        } catch (error) {
            console.error('❌ Rentals analysis failed:', error.message);
            return { error: error.message };
        }
    }

    /**
     * Create required directories
     */
    async createRequiredDirectories() {
        console.log('📁 Creating required directories...');
        
        const directories = [
            'data',
            'data/dhcr',
            'data/cache',
            'data/temp'
        ];
        
        for (const dir of directories) {
            try {
                await fs.mkdir(dir, { recursive: true });
                console.log(`   ✅ Created: ${dir}/`);
            } catch (error) {
                if (error.code !== 'EEXIST') {
                    console.warn(`   ⚠️ Could not create ${dir}:`, error.message);
                }
            }
        }
    }

    /**
     * Ensure DHCR files are available (download if missing)
     */
    async ensureDHCRFiles() {
        console.log('📄 Checking DHCR files (for rentals system)...');
        
        const dhcrUrls = {
            'manhattan': 'https://rentguidelinesboard.cityofnewyork.us/wp-content/uploads/2024/11/2023-DHCR-Bldg-File-Manhattan.pdf',
            'brooklyn': 'https://rentguidelinesboard.cityofnewyork.us/wp-content/uploads/2024/11/2023-DHCR-Bldg-File-Brooklyn.pdf',
            'bronx': 'https://rentguidelinesboard.cityofnewyork.us/wp-content/uploads/2024/11/2023-DHCR-Bldg-File-Bronx.pdf',
            'queens': 'https://rentguidelinesboard.cityofnewyork.us/wp-content/uploads/2024/11/2023-DHCR-Bldg-File-Queens.pdf'
        };

        const dhcrDir = path.join(process.cwd(), 'data', 'dhcr');
        let filesDownloaded = 0;
        let filesExisting = 0;

        for (const [borough, url] of Object.entries(dhcrUrls)) {
            const filename = `2023-DHCR-Bldg-File-${borough.charAt(0).toUpperCase() + borough.slice(1)}.pdf`;
            const filepath = path.join(dhcrDir, filename);

            try {
                // Check if file already exists
                await fs.access(filepath);
                console.log(`   ✅ Found existing: ${filename}`);
                filesExisting++;
            } catch (error) {
                // File doesn't exist, try to download
                console.log(`   📥 Downloading: ${filename}...`);
                
                try {
                    const response = await axios({
                        method: 'GET',
                        url: url,
                        responseType: 'stream',
                        timeout: 60000 // 60 second timeout
                    });

                    const writer = require('fs').createWriteStream(filepath);
                    response.data.pipe(writer);

                    await new Promise((resolve, reject) => {
                        writer.on('finish', resolve);
                        writer.on('error', reject);
                    });

                    console.log(`   ✅ Downloaded: ${filename}`);
                    filesDownloaded++;
                } catch (downloadError) {
                    console.error(`   ❌ Failed to download ${filename}:`, downloadError.message);
                    this.results.dhcrStatus.errors.push({
                        borough,
                        error: downloadError.message
                    });
                }
            }
        }

        this.results.dhcrStatus.downloaded = filesDownloaded;
        console.log(`📊 DHCR Files: ${filesExisting} existing, ${filesDownloaded} downloaded`);

        // Create README if no files available
        if (filesExisting + filesDownloaded === 0) {
            await this.createDHCRReadme(dhcrDir);
        }
    }

    /**
     * Create README for manual DHCR file setup
     */
    async createDHCRReadme(dhcrDir) {
        const readmeContent = `# DHCR Building Files - Manual Setup Required

## Files Needed:
- 2023-DHCR-Bldg-File-Manhattan.pdf
- 2023-DHCR-Bldg-File-Brooklyn.pdf  
- 2023-DHCR-Bldg-File-Bronx.pdf
- 2023-DHCR-Bldg-File-Queens.pdf

## Download URLs:
- Manhattan: https://rentguidelinesboard.cityofnewyork.us/wp-content/uploads/2024/11/2023-DHCR-Bldg-File-Manhattan.pdf
- Brooklyn: https://rentguidelinesboard.cityofnewyork.us/wp-content/uploads/2024/11/2023-DHCR-Bldg-File-Brooklyn.pdf
- Bronx: https://rentguidelinesboard.cityofnewyork.us/wp-content/uploads/2024/11/2023-DHCR-Bldg-File-Bronx.pdf
- Queens: https://rentguidelinesboard.cityofnewyork.us/wp-content/uploads/2024/11/2023-DHCR-Bldg-File-Queens.pdf

## Manual Setup:
1. Download the PDF files from the URLs above
2. Place them in this data/dhcr/ directory
3. Restart the Railway deployment

Auto-download failed, but rentals system will use fallback neighborhoods.
`;

        try {
            await fs.writeFile(path.join(dhcrDir, 'README.md'), readmeContent);
            console.log('📋 Created DHCR setup instructions');
        } catch (error) {
            console.warn('Could not create DHCR README:', error.message);
        }
    }

    /**
     * Check required environment variables
     */
    async checkEnvironmentVariables() {
        console.log('🔧 Checking environment variables...');
        
        const required = [
            'RAPIDAPI_KEY',
            'SUPABASE_URL', 
            'SUPABASE_ANON_KEY'
        ];
        
        const missing = required.filter(key => !process.env[key]);
        
        if (missing.length > 0) {
            console.error('❌ Missing required environment variables:');
            missing.forEach(key => console.error(`   ${key}`));
            throw new Error('Missing required environment variables');
        }
        
        // Check Claude API key
        if (!process.env.ANTHROPIC_API_KEY) {
            console.warn('⚠️ ANTHROPIC_API_KEY not set - Claude analysis will fail');
            console.warn('   Add ANTHROPIC_API_KEY to Railway environment variables');
        }
        
        // Log optional configuration
        const optional = [
            'TEST_NEIGHBORHOOD',
            'RUN_SALES_ONLY',
            'RUN_RENTALS_ONLY',
            'INITIAL_BULK_LOAD',
            'MAX_LISTINGS_PER_NEIGHBORHOOD'
        ];
        
        console.log('📋 Configuration:');
        optional.forEach(key => {
            const value = process.env[key];
            console.log(`   ${key}: ${value || 'not set'}`);
        });
        
        console.log('✅ Environment check completed');
    }

    /**
     * Setup database if needed (for rentals system)
     */
    async setupDatabaseIfNeeded(system) {
        console.log('🔧 Checking database setup...');
        
        try {
            // Test if required tables exist
            const { data, error } = await system.supabase
                .from('undervalued_rentals')
                .select('id')
                .limit(1);

            if (error && error.message.includes('does not exist')) {
                console.log('⚠️ Database schema missing - some features may not work');
                console.log('💡 Please run the SQL schema in Supabase');
            } else {
                console.log('✅ Database schema appears ready');
            }
        } catch (error) {
            console.warn('⚠️ Could not verify database schema:', error.message);
        }
    }

    /**
     * Determine target neighborhoods (with fallbacks)
     */
    async determineTargetNeighborhoods() {
        // Priority 1: Test neighborhood override
        if (process.env.TEST_NEIGHBORHOOD) {
            console.log(`🧪 Using test neighborhood: ${process.env.TEST_NEIGHBORHOOD}`);
            return [process.env.TEST_NEIGHBORHOOD];
        }

        // Priority 2: Manual neighborhood list
        if (process.env.MANUAL_NEIGHBORHOODS) {
            const manual = process.env.MANUAL_NEIGHBORHOODS.split(',').map(n => n.trim());
            console.log(`📝 Using manual neighborhoods: ${manual.join(', ')}`);
            return manual;
        }

        // Priority 3: Fallback to high-priority neighborhoods
        console.log('🎯 Using fallback priority neighborhoods...');
        return this.getHighPriorityNeighborhoods();
    }

    /**
     * Get high-priority neighborhoods for rent-stabilized apartments
     */
    getHighPriorityNeighborhoods() {
        const priorityNeighborhoods = [
            // Manhattan - highest priority
            'east-village', 'lower-east-side', 'chinatown', 'west-village',
            'greenwich-village', 'soho', 'nolita', 'tribeca', 'chelsea',
            'gramercy', 'murray-hill', 'kips-bay', 'flatiron',
            'upper-east-side', 'upper-west-side', 'hells-kitchen',
            
            // Brooklyn - second priority
            'williamsburg', 'dumbo', 'brooklyn-heights', 'cobble-hill',
            'carroll-gardens', 'park-slope', 'fort-greene', 'boerum-hill',
            'prospect-heights', 'crown-heights', 'bedford-stuyvesant',
            'greenpoint', 'bushwick',
            
            // Queens - third priority
            'long-island-city', 'astoria', 'sunnyside', 'woodside',
            'jackson-heights', 'elmhurst', 'forest-hills',
            
            // Bronx - fourth priority
            'mott-haven', 'concourse', 'fordham'
        ];

        // Filter based on borough focus if specified
        if (process.env.FOCUS_BOROUGH) {
            const boroughMap = {
                'manhattan': priorityNeighborhoods.slice(0, 16),
                'brooklyn': priorityNeighborhoods.slice(16, 29),
                'queens': priorityNeighborhoods.slice(29, 36),
                'bronx': priorityNeighborhoods.slice(36)
            };
            
            return boroughMap[process.env.FOCUS_BOROUGH.toLowerCase()] || priorityNeighborhoods;
        }

        // Limit neighborhoods based on mode
        const isTestMode = process.env.NODE_ENV === 'test' || process.env.INITIAL_BULK_LOAD !== 'true';
        return priorityNeighborhoods.slice(0, isTestMode ? 8 : 25);
    }

    /**
     * Log combined results from both systems
     */
    logCombinedResults() {
        const duration = Math.round((Date.now() - this.startTime) / 60000);
        this.results.totalDuration = duration;

        console.log('\n🎉 CLAUDE-INTEGRATED ANALYSIS COMPLETE!');
        console.log('=' .repeat(60));
        console.log(`⏱️ Total duration: ${duration} minutes`);
        
        // DHCR Status
        console.log('\n📄 DHCR FILES STATUS:');
        console.log(`   📥 Downloaded: ${this.results.dhcrStatus.downloaded} files`);
        console.log(`   ❌ Download errors: ${this.results.dhcrStatus.errors.length}`);

        // Sales Results
        if (this.results.salesResults && this.results.salesResults.summary) {
            const salesSummary = this.results.salesResults.summary;
            console.log('\n🏠 CLAUDE SALES ANALYSIS:');
            console.log(`   📊 Undervalued sales found: ${salesSummary.savedToDatabase || 0}`);
            console.log(`   🔍 Properties analyzed: ${salesSummary.totalDetailsFetched || 0}`);
            console.log(`   ⚡ API efficiency: ${salesSummary.cacheHitRate?.toFixed(1) || 0}%`);
        }

        // Rentals Results
        if (this.results.rentalsResults && this.results.rentalsResults.rentStabilized) {
            const rentalsResults = this.results.rentalsResults.rentStabilized;
            console.log('\n🏘️ CLAUDE RENTALS ANALYSIS:');
            console.log(`   📊 Rent-stabilized found: ${rentalsResults.undervaluedStabilizedFound || 0}`);
            console.log(`   🔍 Properties analyzed: ${rentalsResults.totalListingsScanned || 0}`);
            console.log(`   🎯 Neighborhoods processed: ${rentalsResults.neighborhoodsProcessed || 0}`);
        }

        // Combined success message
        const salesFound = this.results.salesResults?.summary?.savedToDatabase || 0;
        const rentalsFound = this.results.rentalsResults?.rentStabilized?.undervaluedStabilizedFound || 0;
        const totalFound = salesFound + rentalsFound;

        if (totalFound > 0) {
            console.log('\n✅ SUCCESS: Found undervalued properties with Claude AI!');
            console.log('🔍 Check your Supabase tables:');
            console.log(`   • undervalued_sales: ${salesFound} deals`);
            console.log(`   • undervalued_rentals: ${rentalsFound} deals`);
        } else {
            console.log('\n📊 No undervalued properties found this run');
            console.log('💡 This is normal in competitive NYC market');
        }

        console.log('\n🎯 Claude-integrated Railway deployment completed successfully');
    }

    /**
     * Log sales-only results
     */
    logSalesOnlyResults() {
        const duration = Math.round((Date.now() - this.startTime) / 60000);
        
        console.log('\n🎉 CLAUDE SALES ANALYSIS COMPLETE!');
        console.log('=' .repeat(60));
        console.log(`⏱️ Duration: ${duration} minutes`);
        
        if (this.results.salesResults && this.results.salesResults.summary) {
            const summary = this.results.salesResults.summary;
            console.log(`📊 Undervalued sales found: ${summary.savedToDatabase || 0}`);
            console.log(`🔍 Properties analyzed: ${summary.totalDetailsFetched || 0}`);
            console.log(`⚡ API efficiency: ${summary.cacheHitRate?.toFixed(1) || 0}%`);
            
            if (summary.savedToDatabase > 0) {
                console.log('\n✅ SUCCESS: Check your Supabase undervalued_sales table');
            }
        }
    }

    /**
     * Log rentals-only results
     */
    logRentalsOnlyResults() {
        const duration = Math.round((Date.now() - this.startTime) / 60000);
        
        console.log('\n🎉 CLAUDE RENTALS ANALYSIS COMPLETE!');
        console.log('=' .repeat(60));
        console.log(`⏱️ Duration: ${duration} minutes`);
        
        if (this.results.rentalsResults && this.results.rentalsResults.rentStabilized) {
            const results = this.results.rentalsResults.rentStabilized;
            console.log(`📊 Rent-stabilized found: ${results.undervaluedStabilizedFound || 0}`);
            console.log(`🔍 Properties analyzed: ${results.totalListingsScanned || 0}`);
            console.log(`🎯 Neighborhoods processed: ${results.neighborhoodsProcessed || 0}`);
            
            if (results.undervaluedStabilizedFound > 0) {
                console.log('\n✅ SUCCESS: Check your Supabase undervalued_rentals table');
            }
        }
    }

    /**
     * Utility delay function
     */
    async delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

// Main execution function for Railway
async function main() {
    try {
        console.log('🚀 CLAUDE-INTEGRATED RAILWAY DEPLOYMENT STARTING...');
        
        const runner = new ClaudeIntegratedRailwayRunner();
        const results = await runner.runBothClaudeSystems();
        
        // Keep process alive to see results
        console.log('\n⏰ Keeping process alive for 30 seconds to view results...');
        await runner.delay(30000);
        
        console.log('✅ Claude-integrated analysis complete - Railway deployment successful');
        process.exit(0);

    } catch (error) {
        console.error('💥 Claude Railway deployment failed:', error.message);
        console.error('\n💡 Common fixes:');
        console.error('   • Add ANTHROPIC_API_KEY to Railway environment');
        console.error('   • Ensure both Claude system files are deployed');
        console.error('   • Check Supabase connection');
        console.error('   • Set TEST_NEIGHBORHOOD=soho for testing');
        
        process.exit(1);
    }
}

// Export for testing
module.exports = ClaudeIntegratedRailwayRunner;

// Run if executed directly (Railway entry point)
if (require.main === module) {
    main().catch(error => {
        console.error('💥 Claude Railway runner crashed:', error);
        process.exit(1);
    });
}
