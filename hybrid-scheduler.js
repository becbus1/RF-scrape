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
