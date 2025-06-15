// test-scraper.js
// Comprehensive test suite for NYC Redfin scraper

const RedfinAPIScraper = require('./redfin-scraper.js');
const UndervaluedPropertyFinder = require('./undervalued-property-finder.js');

async function runNYCTestSuite() {
    console.log('🗽 Starting NYC Redfin API scraper test suite...\n');
    
    const scraper = new RedfinAPIScraper();
    const finder = new UndervaluedPropertyFinder();
    
    const testResults = {
        passed: 0,
        failed: 0,
        tests: []
    };
    
    // Helper function to run a test
    async function runTest(testName, testFunction) {
        console.log(`🧪 Testing: ${testName}`);
        try {
            const result = await testFunction();
            console.log(`✅ PASSED: ${testName}`);
            testResults.passed++;
            testResults.tests.push({ name: testName, status: 'PASSED', result });
            return result;
        } catch (error) {
            console.log(`❌ FAILED: ${testName} - ${error.message}`);
            testResults.failed++;
            testResults.tests.push({ name: testName, status: 'FAILED', error: error.message });
            return null;
        }
    }
    
    // Test 1: NYC Region ID lookup
    const manhattanRegion = await runTest('NYC Region ID Lookup (Manhattan)', async () => {
        const result = await scraper.getRegionId('Manhattan, NY');
        if (!result.id || !result.type || !result.name) {
            throw new Error('Invalid Manhattan region response structure');
        }
        if (!result.name.toLowerCase().includes('manhattan')) {
            throw new Error('Region name does not contain Manhattan');
        }
        return result;
    });
    
    if (!manhattanRegion) {
        console.log('\n💥 Cannot continue without valid NYC region. Exiting...');
        return testResults;
    }
    
    // Test 2: NYC CSV Data Scraping
    await runTest('NYC CSV Listings Scrape', async () => {
        const listings = await scraper.scrapeListingsCSV(manhattanRegion.id, manhattanRegion.type, {
            limit: 10,
            status: 9
        });
        
        if (!Array.isArray(listings) || listings.length === 0) {
            throw new Error('No NYC listings returned from CSV API');
        }
        
        // Validate first listing structure
        const firstListing = listings[0];
        const requiredFields = ['address', 'price', 'city'];
        const missingFields = requiredFields.filter(field => !firstListing[field]);
        
        if (missingFields.length > 0) {
            throw new Error(`Missing required fields: ${missingFields.join(', ')}`);
        }
        
        // Check if it's actually NYC data
        const cityState = `${firstListing.city} ${firstListing.state_or_province}`.toLowerCase();
        if (!cityState.includes('ny') && !cityState.includes('new york')) {
            throw new Error('Data does not appear to be from NYC');
        }
        
        return { count: listings.length, sample: firstListing };
    });
    
    // Test 3: NYC Property Search Integration
    await runTest('NYC Property Search Integration', async () => {
        const results = await scraper.searchProperties('Brooklyn, NY', {
            limit: 5,
            minPrice: 400000,
            status: 9
        });
        
        if (!results.region || !results.listings || !Array.isArray(results.listings)) {
            throw new Error('Invalid NYC search results structure');
        }
        
        if (!results.region.name.toLowerCase().includes('brooklyn')) {
            throw new Error('Region name does not match Brooklyn search');
        }
        
        return {
            region: results.region.name,
            listingCount: results.listings.length,
            total: results.total
        };
    });
    
    // Test 4: All NYC Boroughs Data
    await runTest('All NYC Boroughs Data Fetch', async () => {
        const allNYC = await scraper.getAllNYCData({
            limit: 3,
            status: 9
        });
        
        const expectedBoroughs = ['Manhattan, NY', 'Brooklyn, NY', 'Queens, NY', 'Bronx, NY', 'Staten Island, NY'];
        const foundBoroughs = Object.keys(allNYC);
        
        const missingBoroughs = expectedBoroughs.filter(borough => !foundBoroughs.includes(borough));
        if (missingBoroughs.length > 0) {
            throw new Error(`Missing borough data: ${missingBoroughs.join(', ')}`);
        }
        
        return {
            boroughsFound: foundBoroughs.length,
            sampleData: Object.keys(allNYC).map(borough => ({
                borough,
                listingCount: allNYC[borough].listings ? allNYC[borough].listings.length : 0
            }))
        };
    });
    
    // Test 5: NYC Undervalued Property Detection
    await runTest('NYC Undervalued Property Detection', async () => {
        const results = await finder.findUndervaluedProperties('Queens, NY', {
            limit: 20,
            minDiscountPercent: 10, // Lower threshold for testing
            maxDaysOnMarket: 120,
            maxPrice: 1500000
        });
        
        if (!results || !results.location || !Array.isArray(results.undervaluedProperties)) {
            throw new Error('Invalid undervalued property results structure');
        }
        
        if (!results.location.toLowerCase().includes('queens')) {
            throw new Error('Results location does not match Queens search');
        }
        
        // Check if any properties were analyzed
        if (results.totalListings === 0) {
            throw new Error('No listings were analyzed');
        }
        
        return {
            location: results.location,
            totalListings: results.totalListings,
            undervaluedCount: results.undervaluedCount,
            hasMarketData: !!results.marketData
        };
    });
    
    // Test 6: NYC Property Scoring System
    await runTest('NYC Property Scoring System', async () => {
        // Create a mock property for scoring
        const mockProperty = {
            address: '123 Test St, Brooklyn, NY',
            price: 800000,
            sqft: 1000,
            beds: '2',
            baths: '1',
            daysOnMarket: 5,
            percentBelowMarket: 20,
            distressSignals: ['motivated seller', 'as-is'],
            warningTags: [],
            comparisonLevel: 'NYC Neighborhood + Bedrooms',
            comparableCount: 15
        };
        
        const scored = finder.calculatePropertyScore(mockProperty);
        
        if (!scored.score || !scored.grade || !scored.reasoning) {
            throw new Error('Scoring system did not return required fields');
        }
        
        if (scored.score < 0 || scored.score > 100) {
            throw new Error(`Score ${scored.score} is outside valid range 0-100`);
        }
        
        const validGrades = ['A+', 'A', 'B', 'C', 'D', 'F'];
        if (!validGrades.includes(scored.grade)) {
            throw new Error(`Invalid grade: ${scored.grade}`);
        }
        
        return {
            score: scored.score,
            grade: scored.grade,
            hasReasoning: scored.reasoning.length > 0
        };
    });
    
    // Test 7: NYC-Specific Keywords Detection
    await runTest('NYC-Specific Keywords Detection', async () => {
        const testDescriptions = [
            'Beautiful prewar building with original details, motivated seller',
            'Rent stabilized tenant in place, needs gut renovation',
            'Walk up building, no elevator, cash only sale',
            'Co-op conversion, sponsor unit available'
        ];
        
        let totalDistressSignals = 0;
        let totalWarningSignals = 0;
        
        testDescriptions.forEach(description => {
            const distress = finder.findDistressSignals(description);
            const warnings = finder.findWarningSignals(description);
            totalDistressSignals += distress.length;
            totalWarningSignals += warnings.length;
        });
        
        if (totalDistressSignals === 0) {
            throw new Error('No distress signals detected in test descriptions');
        }
        
        return {
            totalDistressSignals,
            totalWarningSignals,
            testDescriptions: testDescriptions.length
        };
    });
    
    // Test 8: Database Format Conversion
    await runTest('Database Format Conversion', async () => {
        const mockResults = {
            undervaluedProperties: [
                {
                    address: '456 Brooklyn Ave, Brooklyn, NY',
                    price: 750000,
                    beds: '3',
                    sqft: 1200,
                    zip: '11201',
                    url: 'https://www.redfin.com/test-property',
                    score: 85,
                    percentBelowMarket: 18.5,
                    warningTags: ['needs work']
                }
            ]
        };
        
        const dbFormat = finder.formatForDatabase(mockResults);
        
        if (!Array.isArray(dbFormat) || dbFormat.length === 0) {
            throw new Error('Database format conversion failed');
        }
        
        const dbProperty = dbFormat[0];
        const requiredDbFields = ['address', 'price', 'beds', 'sqft', 'zip', 'link', 'score', 'percent_below_market', 'warning_tags'];
        const missingDbFields = requiredDbFields.filter(field => !(field in dbProperty));
        
        if (missingDbFields.length > 0) {
            throw new Error(`Missing database fields: ${missingDbFields.join(', ')}`);
        }
        
        // Check price formatting
        if (!dbProperty.price.startsWith('$')) {
            throw new Error('Price not properly formatted for database');
        }
        
        return {
            conversionSuccessful: true,
            sampleProperty: dbProperty
        };
    });
    
    // Test 9: Error Handling
    await runTest('Error Handling', async () => {
        try {
            await scraper.getRegionId('NonexistentNYCLocation12345XYZ');
            throw new Error('Should have thrown an error for invalid NYC location');
        } catch (error) {
            if (error.message.includes('No exact match found')) {
                return { errorHandling: 'correct' };
            }
            throw new Error(`Unexpected error message: ${error.message}`);
        }
    });
    
    // Test 10: Rate Limiting Behavior
    await runTest('Rate Limiting Behavior', async () => {
        const startTime = Date.now();
        
        // Make two quick requests
        await scraper.getRegionId('Manhattan, NY');
        await scraper.getRegionId('Brooklyn, NY');
        
        const elapsed = Date.now() - startTime;
        const expectedMinTime = scraper.rateLimitDelay; // Should respect rate limit
        
        return {
            elapsed: elapsed,
            rateLimitDelay: scraper.rateLimitDelay,
            respectsRateLimit: elapsed >= expectedMinTime
        };
    });
    
    // Final Results
    console.log('\n' + '='.repeat(60));
    console.log('🗽 NYC TEST SUITE SUMMARY');
    console.log('='.repeat(60));
    console.log(`✅ Tests Passed: ${testResults.passed}`);
    console.log(`❌ Tests Failed: ${testResults.failed}`);
    console.log(`📈 Success Rate: ${((testResults.passed / (testResults.passed + testResults.failed)) * 100).toFixed(1)}%`);
    
    if (testResults.failed === 0) {
        console.log('\n🎉 All NYC tests passed! The scraper is working correctly for NYC data.');
        console.log('\n🗽 You can now use the NYC scraper with confidence!');
        
        // Show a quick NYC example
        console.log('\n📋 Quick NYC Example Usage:');
        console.log('```javascript');
        console.log('const finder = new UndervaluedPropertyFinder();');
        console.log('const results = await finder.findUndervaluedProperties("Manhattan, NY", {');
        console.log('    minDiscountPercent: 15,');
        console.log('    maxPrice: 2500000,');
        console.log('    limit: 100');
        console.log('});');
        console.log('console.log(`Found ${results.undervaluedCount} undervalued NYC properties`);');
        console.log('```');
    } else {
        console.log('\n⚠️  Some tests failed. Check the issues above.');
        console.log('🔧 Common fixes for NYC scraping:');
        console.log('   - Ensure internet connection is stable');
        console.log('   - Check if Redfin has changed their NYC API endpoints');
        console.log('   - Verify axios dependency is installed');
        console.log('   - NYC market may be very competitive (fewer undervalued properties)');
        console.log('   - Try running tests again (could be temporary network issues)');
    }
    
    // Detailed test breakdown
    console.log('\n📋 Detailed NYC Test Results:');
    testResults.tests.forEach((test, index) => {
        const status = test.status === 'PASSED' ? '✅' : '❌';
        console.log(`${index + 1}. ${status} ${test.name}`);
        if (test.status === 'FAILED') {
            console.log(`   Error: ${test.error}`);
        }
    });
    
    return testResults;
}

// NYC Demo function to show real usage
async function runNYCDemo() {
    console.log('\n' + '='.repeat(60));
    console.log('🗽 NYC DEMO: Real Scraping Example');
    console.log('='.repeat(60));
    
    const finder = new UndervaluedPropertyFinder();
    
    try {
        console.log('🔍 Searching for undervalued properties in Brooklyn under $1.2M...');
        
        const results = await finder.findUndervaluedProperties('Brooklyn, NY', {
            limit: 20,
            maxPrice: 1200000,
            status: 9,
            minBeds: 1,
            minDiscountPercent: 12 // Slightly lower for NYC market
        });
        
        console.log(`\n📊 Found ${results.totalListings} total Brooklyn properties`);
        console.log(`📍 Region: ${results.region.name} (ID: ${results.region.id})`);
        console.log(`🎯 Undervalued properties: ${results.undervaluedCount}`);
        
        if (results.undervaluedCount > 0) {
            console.log(`📋 Showing first ${Math.min(results.undervaluedCount, 5)} undervalued properties:\n`);
            
            results.undervaluedProperties.slice(0, 5).forEach((listing, index) => {
                console.log(`${index + 1}. 🏠 ${listing.address}`);
                console.log(`   💰 Price: $${listing.price.toLocaleString()} (${listing.percentBelowMarket.toFixed(1)}% below market)`);
                console.log(`   🏆 Score: ${listing.score}/100 (Grade: ${listing.grade})`);
                console.log(`   🛏️ ${listing.beds} bed, ${listing.baths} bath`);
                console.log(`   📏 ${listing.sqft.toLocaleString()} sq ft`);
                console.log(`   📅 ${listing.daysOnMarket} days on market`);
                console.log(`   📍 ZIP: ${listing.zip}`);
                if (listing.distressSignals.length > 0) {
                    console.log(`   🚨 Signals: ${listing.distressSignals.join(', ')}`);
                }
                console.log(`   🔗 ${listing.url}`);
                console.log('');
            });
        } else {
            console.log('\n📊 No undervalued properties found with current criteria');
            console.log('💡 Try adjusting criteria:');
            console.log('   - Lower minDiscountPercent to 10%');
            console.log('   - Increase maxPrice to $1.5M');
            console.log('   - Increase maxDaysOnMarket to 120 days');
        }
        
        // Save demo data
        await finder.saveResults({
            demoTimestamp: new Date().toISOString(),
            searchCriteria: {
                location: 'Brooklyn, NY',
                maxPrice: 1200000,
                minBeds: 1,
                minDiscountPercent: 12
            },
            results: results
        }, 'nyc-demo-results.json');
        
        console.log('💾 NYC demo results saved to nyc-demo-results.json');
        
    } catch (error) {
        console.error('❌ NYC demo failed:', error.message);
    }
}

// Main execution
async function main() {
    const args = process.argv.slice(2);
    
    if (args.includes('--demo-only')) {
        await runNYCDemo();
    } else if (args.includes('--test-only')) {
        await runNYCTestSuite();
    } else {
        // Run both tests and demo
        const testResults = await runNYCTestSuite();
        
        if (testResults.failed === 0) {
            await runNYCDemo();
        } else {
            console.log('\n⚠️ Skipping demo due to test failures. Fix issues first.');
        }
    }
}

// Run the script
if (require.main === module) {
    main().catch(error => {
        console.error('💥 NYC test script crashed:', error);
        process.exit(1);
    });
}

module.exports = { runNYCTestSuite, runNYCDemo };
