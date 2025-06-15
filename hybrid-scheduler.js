// hybrid-scheduler.js
// Weekly automation for hybrid Redfin analysis

require('dotenv').config();
const cron = require('node-cron');
const HybridPropertyTracker = require('./hybrid-supabase-integration.js');

class HybridPropertyScheduler {
    constructor() {
        this.tracker = new HybridPropertyTracker(
            process.env.SUPABASE_URL,
            process.env.SUPABASE_ANON_KEY
        );
        
        this.isRunning = false;
        this.lastRunTime = null;
        this.runCount = 0;
    }

    /**
     * Start the hybrid property analysis scheduler
     */
    startHybridScheduler() {
        console.log('🚀 Starting Hybrid Property Analysis Scheduler...\n');

        // Weekly analysis every Wednesday at 8:00 AM EST (when Redfin updates data)
        cron.schedule('0 8 * * 3', async () => {
            await this.runScheduledAnalysis('Weekly Hybrid Analysis');
        }, {
            scheduled: true,
            timezone: "America/New_York"
        });

        // Daily quick check for new distress signals at 6:00 PM EST
        cron.schedule('0 18 * * *', async () => {
            await this.runDistressSignalCheck();
        }, {
            scheduled: true,
            timezone: "America/New_York"
        });

        // Monthly cleanup on first Sunday at 3:00 AM EST
        cron.schedule('0 3 1-7 * 0', async () => {
            await this.runMonthlyCleanup();
        }, {
            scheduled: true,
            timezone: "America/New_York"
        });

        // Hourly health check
        cron.schedule('0 * * * *', () => {
            this.logHealthStatus();
        });

        console.log('📅 Hybrid Scheduler started with jobs:');
        console.log('   📊 Weekly analysis: Wednesday 8:00 AM EST (when Redfin updates data)');
        console.log('   🚨 Daily distress check: 6:00 PM EST');
        console.log('   🧹 Monthly cleanup: First Sunday 3:00 AM EST');
        console.log('   💓 Hourly health check');
        console.log('   🎯 Focus: Public data + targeted description enhancement');
        console.log('\n⚡ Hybrid Scheduler is now running. Press Ctrl+C to stop.\n');
    }

    /**
     * Run scheduled hybrid analysis
     */
    async runScheduledAnalysis(jobName) {
        if (this.isRunning) {
            console.log(`⚠️ ${jobName}: Skipping - previous analysis still running`);
            return;
        }

        this.isRunning = true;
        this.runCount++;
        
        console.log(`\n${'='.repeat(70)}`);
        console.log(`📊 ${jobName} #${this.runCount} - ${new Date().toISOString()}`);
        console.log(`${'='.repeat(70)}`);

        try {
            const startTime = Date.now();
            
            // Run hybrid analysis with optimized settings
            const results = await this.tracker.runWeeklyAnalysis({
                minDiscountPercent: 15,
                maxDaysOnMarket: 90,
                minPrice: 300000,
                maxPrice: 2500000
            });

            const duration = Date.now() - startTime;
            this.lastRunTime = new Date().toISOString();

            // Log comprehensive results
            console.log(`\n✅ ${jobName} completed successfully!`);
            console.log(`⏱️ Duration: ${Math.round(duration / 1000 / 60)} minutes`);
            console.log(`📊 Properties analyzed: ${results.summary.totalPropertiesAnalyzed}`);
            console.log(`🎯 Undervalued found: ${results.summary.undervaluedFound}`);
            console.log(`💾 New listings added: ${results.summary.newListingsAdded}`);
            console.log(`📝 Descriptions enhanced: ${results.summary.descriptionsEnhanced}`);

            // Send notification if significant results
            if (results.summary.newListingsAdded > 5) {
                await this.sendNotification(`🗽 Found ${results.summary.newListingsAdded} new undervalued NYC properties!`);
            }

        } catch (error) {
            console.error(`❌ ${jobName} failed:`, error.message);
            await this.sendNotification(`🚨 Weekly analysis failed: ${error.message}`);
        } finally {
            this.isRunning = false;
        }
    }

    /**
     * Run distress signal check
     */
    async runDistressSignalCheck() {
        console.log('\n🚨 Running daily distress signal check...');
        
        try {
            const properties = await this.tracker.getPropertiesWithDistressSignals(10);
            
            if (properties.length > 0) {
                console.log(`✅ Found ${properties.length} properties with distress signals`);
                
                // Log top properties with distress signals
                properties.slice(0, 3).forEach((prop, i) => {
                    console.log(`${i + 1}. ${prop.address} - ${prop.price} (Score: ${prop.score})`);
                    console.log(`   Signals: ${prop.distress_signals.join(', ')}`);
                });
            } else {
                console.log('📊 No new properties with distress signals found');
            }
        } catch (error) {
            console.error('❌ Distress signal check failed:', error.message);
        }
    }

    /**
     * Run monthly cleanup
     */
    async runMonthlyCleanup() {
        console.log('\n🧹 Running monthly cleanup...');
        
        try {
            const deletedCount = await this.tracker.cleanupOldData(60);
            console.log(`✅ Monthly cleanup completed: ${deletedCount} old records removed`);
            
            // Get current metrics
            const metrics = await this.tracker.getAnalysisMetrics();
            if (metrics) {
                console.log(`📊 Current database: ${metrics.totalProperties} properties`);
                console.log(`🏆 Excellent deals: ${metrics.scoreDistribution.excellent}`);
            }
        } catch (error) {
            console.error('❌ Monthly cleanup failed:', error.message);
        }
    }

    /**
     * Log health status
     */
    logHealthStatus() {
        const now = new Date();
        const uptime = process.uptime();
        const memUsage = process.memoryUsage();
        
        console.log(`💓 Health Check: ${now.toISOString()}`);
        console.log(`   Uptime: ${Math.round(uptime / 3600)}h ${Math.round((uptime % 3600) / 60)}m`);
        console.log(`   Memory: ${Math.round(memUsage.rss / 1024 / 1024)}MB`);
        console.log(`   Last run: ${this.lastRunTime || 'Never'}`);
        console.log(`   Total runs: ${this.runCount}`);
        console.log(`   Status: ${this.isRunning ? 'Running Analysis' : 'Idle'}`);
    }

    /**
     * Send notification
     */
    async sendNotification(message) {
        console.log(`🔔 NOTIFICATION: ${message}`);
        
        // Optional: Send to webhook if configured
        const webhookUrl = process.env.WEBHOOK_URL;
        if (webhookUrl) {
            try {
                const axios = require('axios');
                await axios.post(webhookUrl, {
                    text: `🗽 NYC Property Analyzer: ${message}`,
                    timestamp: new Date().toISOString()
                });
                console.log('📤 Notification sent to webhook');
            } catch (error) {
                console.error('❌ Failed to send webhook notification:', error.message);
            }
        }
    }
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
        console.error('\n💡 Set these in Railway Dashboard → Variables');
        process.exit(1);
    }

    if (args.includes('--test')) {
        console.log('🧪 Testing scheduler setup...');
        const scheduler = new HybridPropertyScheduler();
        scheduler.logHealthStatus();
        console.log('✅ Scheduler test completed');
        return;
    }

    // Start the hybrid scheduler
    const scheduler = new HybridPropertyScheduler();
    
    // Handle graceful shutdown
    process.on('SIGINT', () => {
        console.log('\n📥 Received shutdown signal...');
        process.exit(0);
    });

    process.on('SIGTERM', () => {
        console.log('\n📥 Received termination signal...');
        process.exit(0);
    });

    // Start the scheduler
    scheduler.startHybridScheduler();
    
    // Add immediate test run after 2 minutes of startup
    console.log('\n🧪 Immediate test scrape will run in 2 minutes...');
    setTimeout(async () => {
        console.log('\n🚀 Starting immediate test scrape...');
        try {
            await scheduler.runScheduledAnalysis('Immediate Test Analysis');
            console.log('✅ Test scrape completed successfully!');
            console.log('📊 Check your Supabase table for new data!');
        } catch (error) {
            console.error('❌ Test scrape failed:', error.message);
        }
    }, 120000); // Wait 2 minutes after startup
}

// Run if executed directly
if (require.main === module) {
    main().catch(error => {
        console.error('💥 Scheduler crashed:', error);
        process.exit(1);
    });
}

module.exports = HybridPropertyScheduler;
