// cron-scheduler.js
// Automated daily NYC property scraping with cron jobs

require('dotenv').config();
const cron = require('node-cron');
const NYCPropertyTracker = require('./supabase-integration.js');

class NYCPropertyScrapingScheduler {
    constructor() {
        this.tracker = new NYCPropertyTracker(
            process.env.SUPABASE_URL,
            process.env.SUPABASE_ANON_KEY
        );
        
        this.isRunning = false;
        this.lastRunTime = null;
        this.runCount = 0;
        
        // NYC-focused locations
        this.nycLocations = [
            'Manhattan, NY',
            'Brooklyn, NY',
            'Queens, NY',
            'Bronx, NY',
            'Staten Island, NY'
        ];
    }

    /**
     * Start the scheduled NYC property scraping
     */
    startNYCScheduler() {
        console.log('🗽 Starting NYC Property Scraping Scheduler...\n');

        // Daily morning scrape at 6:00 AM EST
        cron.schedule('0 6 * * *', async () => {
            await this.runScheduledNYCScrape('Daily Morning NYC Scrape');
        }, {
            scheduled: true,
            timezone: "America/New_York" // NYC timezone
        });

        // Evening scrape at 7:00 PM EST (NYC market is very active)
        cron.schedule('0 19 * * *', async () => {
            await this.runScheduledNYCScrape('Daily Evening NYC Scrape');
        }, {
            scheduled: true,
            timezone: "America/New_York"
        });

        // Weekly cleanup on Sundays at 2:00 AM EST
        cron.schedule('0 2 * * 0', async () => {
            await this.runWeeklyCleanup();
        }, {
            scheduled: true,
            timezone: "America/New_York"
        });

        // Hourly health check
        cron.schedule('0 * * * *', () => {
            this.logNYCHealthStatus();
        });

        console.log('📅 NYC Scheduler started with the following jobs:');
        console.log('   ⏰ Daily NYC scrape: 6:00 AM and 7:00 PM EST');
        console.log('   🧹 Weekly cleanup: Sunday 2:00 AM EST');
        console.log('   💓 Hourly health check');
        console.log('   🗽 Target: All 5 NYC boroughs');
        console.log('\n⚡ NYC Scheduler is now running. Press Ctrl+C to stop.\n');
    }

    /**
     * Run a scheduled NYC property scrape
     */
    async runScheduledNYCScrape(jobName) {
        if (this.isRunning) {
            console.log(`⚠️ ${jobName}: Skipping - previous NYC scrape still running`);
            return;
        }

        this.isRunning = true;
        this.runCount++;
        
        console.log(`\n${'='.repeat(70)}`);
        console.log(`🗽 ${jobName} #${this.runCount} - ${new Date().toISOString()}`);
        console.log(`${'='.repeat(70)}`);

        try {
            const startTime = Date.now();
            
            // Run the NYC scrape with NYC-optimized settings
            const results = await this.tracker.runDailyNYCScrape(this.nycLocations, {
                minDiscountPercent: 15,
                maxDaysOnMarket: 90,
                maxPrice: 3000000, // NYC pricing
                limit: 350
            });

            const duration = Date.now() - startTime;
            this.lastRunTime = new Date().toISOString();

            // Log success
            console.log(`\n✅ ${jobName} completed successfully!`);
            console.log(`⏱️ Duration: ${Math.round(duration / 1000 / 60)} minutes`);
            console.log(`🗽 NYC Summary: ${results.summary.newListingsAdded} new undervalued properties added`);

            // Send alert if many NYC properties found
            if (results.summary.newListingsAdded > 15) {
                await this.sendNYCAlert(`🗽 Found ${results.summary.newListingsAdded} new undervalued NYC properties!`);
            }

            // Log borough-specific results
            this.logBoroughResults(results);

        } catch (error) {
            console.error(`❌ ${jobName} failed:`, error.message);
            await this.sendNYCAlert(`🚨 NYC property scrape failed: ${error.message}`);
        } finally {
            this.isRunning = false;
        }
    }

    /**
     * Log results by NYC borough
     */
    logBoroughResults(results) {
        console.log('\n🗽 NYC Borough Breakdown:');
        
        // This would need to be tracked in the actual scraping
        // For now, just show overall stats
        console.log(`   📊 Total listings analyzed: ${results.summary.totalListingsAnalyzed}`);
        console.log(`   🎯 Total undervalued found: ${results.summary.totalUndervaluedFound}`);
        console.log(`   💾 New listings saved: ${results.summary.newListingsAdded}`);
        
        if (results.summary.errors && results.summary.errors.length > 0) {
            console.log(`   ❌ Borough errors: ${results.summary.errors.length}`);
            results.summary.errors.forEach(error => {
                console.log(`      - ${error.location}: ${error.error}`);
            });
        }
    }

    /**
     * Run weekly database cleanup
     */
    async runWeeklyCleanup() {
        console.log('\n🧹 Running weekly NYC database cleanup...');
        
        try {
            const deletedCount = await this.tracker.cleanupOldListings(30);
            console.log(`✅ Weekly cleanup completed: ${deletedCount} old NYC listings removed`);
            
            // Also get and log current stats
            const stats = await this.tracker.getNYCMarketStats();
            if (stats) {
                console.log(`📊 Current database: ${stats.totalListings} total NYC properties`);
                console.log(`🏆 Top scoring properties: ${stats.scoreDistribution.excellent} excellent deals`);
            }
        } catch (error) {
            console.error('❌ Weekly cleanup failed:', error.message);
        }
    }

    /**
     * Log NYC-specific health status
     */
    logNYCHealthStatus() {
        const now = new Date();
        const uptime = process.uptime();
        const memUsage = process.memoryUsage();
        
        console.log(`💓 NYC Health Check: ${now.toISOString()}`);
        console.log(`   Uptime: ${Math.round(uptime / 3600)}h ${Math.round((uptime % 3600) / 60)}m`);
        console.log(`   Memory: ${Math.round(memUsage.rss / 1024 / 1024)}MB`);
        console.log(`   Last NYC run: ${this.lastRunTime || 'Never'}`);
        console.log(`   Total NYC runs: ${this.runCount}`);
        console.log(`   Status: ${this.isRunning ? 'Running NYC Scrape' : 'Idle'}`);
        console.log(`   Target: ${this.nycLocations.length} NYC boroughs`);
    }

    /**
     * Send NYC-specific alert/notification
     */
    async sendNYCAlert(message) {
        console.log(`🗽 NYC ALERT: ${message}`);
        
        // Example: Send to webhook
        const webhookUrl = process.env.WEBHOOK_URL;
        if (webhookUrl) {
            try {
                const axios = require('axios');
                await axios.post(webhookUrl, {
                    text: `🗽 NYC Property Scraper Alert: ${message}`,
                    timestamp: new Date().toISOString(),
                    boroughs: this.nycLocations
                });
                console.log('📤 NYC alert sent to webhook');
            } catch (error) {
                console.error('❌ Failed to send NYC webhook alert:', error.message);
            }
        }
    }

    /**
     * Stop the NYC scheduler gracefully
     */
    stopNYCScheduler() {
        console.log('\n⏹️ Stopping NYC scheduler...');
        process.exit(0);
    }
}

// Manual NYC scrape function for testing
async function runManualNYCScrape() {
    console.log('🗽 Running manual NYC test scrape...\n');
    
    const tracker = new NYCPropertyTracker(
        process.env.SUPABASE_URL,
        process.env.SUPABASE_ANON_KEY
    );

    try {
        // Test with just Manhattan first
        const results = await tracker.runDailyNYCScrape(['Manhattan, NY'], {
            minDiscountPercent: 15,
            maxDaysOnMarket: 60,
            maxPrice: 2500000,
            limit: 50
        });

        console.log('\n✅ Manual NYC scrape completed!');
        console.log(`📊 Found ${results.summary.newListingsAdded} new undervalued Manhattan properties`);
        
        if (results.summary.newListingsAdded > 0) {
            console.log('🏆 Success! NYC scraper is working properly.');
        } else {
            console.log('💡 No new properties found - this is normal for competitive NYC market');
            console.log('   Try lowering minDiscountPercent or increasing maxDaysOnMarket');
        }
        
    } catch (error) {
        console.error('❌ Manual NYC scrape failed:', error.message);
    }
}

// Enhanced NYC scrape with borough-specific settings
async function runBoroughSpecificScrape() {
    console.log('🗽 Running borough-specific NYC scrape...\n');
    
    const tracker = new NYCPropertyTracker(
        process.env.SUPABASE_URL,
        process.env.SUPABASE_ANON_KEY
    );

    const boroughSettings = {
        'Manhattan, NY': { maxPrice: 2500000, minDiscountPercent: 15 },
        'Brooklyn, NY': { maxPrice: 1500000, minDiscountPercent: 15 },
        'Queens, NY': { maxPrice: 1200000, minDiscountPercent: 15 },
        'Bronx, NY': { maxPrice: 800000, minDiscountPercent: 12 },
        'Staten Island, NY': { maxPrice: 900000, minDiscountPercent: 12 }
    };

    let totalFound = 0;

    for (const [borough, settings] of Object.entries(boroughSettings)) {
        try {
            console.log(`🔍 Scraping ${borough} with max price $${settings.maxPrice.toLocaleString()}...`);
            
            const results = await tracker.runDailyNYCScrape([borough], {
                ...settings,
                maxDaysOnMarket: 90,
                limit: 100
            });

            console.log(`✅ ${borough}: ${results.summary.newListingsAdded} new properties`);
            totalFound += results.summary.newListingsAdded;

        } catch (error) {
            console.error(`❌ Error with ${borough}:`, error.message);
        }

        // Rate limiting between boroughs
        await new Promise(resolve => setTimeout(resolve, 3000));
    }

    console.log(`\n🗽 Borough-specific scrape complete: ${totalFound} total new properties found`);
}

// Main execution
async function main() {
    const args = process.argv.slice(2);

    // Check environment variables
    if (!process.env.SUPABASE_URL || !process.env.SUPABASE_ANON_KEY) {
        console.error('❌ Missing required environment variables!');
        console.error('\n📋 Required environment variables:');
        console.error('   SUPABASE_URL=your_supabase_project_url');
        console.error('   SUPABASE_ANON_KEY=your_supabase_anon_key');
        console.error('   WEBHOOK_URL=your_notification_webhook (optional)');
        console.error('\n💡 Create a .env file with these variables for NYC scraping');
        process.exit(1);
    }

    if (args.includes('--manual')) {
        await runManualNYCScrape();
        return;
    }

    if (args.includes('--borough-specific')) {
        await runBoroughSpecificScrape();
        return;
    }

    if (args.includes('--test')) {
        console.log('🧪 Testing NYC scheduler setup...');
        const scheduler = new NYCPropertyScrapingScheduler();
        scheduler.logNYCHealthStatus();
        console.log('✅ NYC scheduler test completed');
        return;
    }

    // Default: start the NYC scheduler
    const scheduler = new NYCPropertyScrapingScheduler();
    
    // Handle graceful shutdown
    process.on('SIGINT', () => {
        console.log('\n📥 Received shutdown signal...');
        scheduler.stopNYCScheduler();
    });

    process.on('SIGTERM', () => {
        console.log('\n📥 Received termination signal...');
        scheduler.stopNYCScheduler();
    });

    // Start the NYC scheduler
    scheduler.startNYCScheduler();
}

// Run if executed directly
if (require.main === module) {
    main().catch(error => {
        console.error('💥 NYC scheduler crashed:', error);
        process.exit(1);
    });
}

module.exports = NYCPropertyScrapingScheduler;
