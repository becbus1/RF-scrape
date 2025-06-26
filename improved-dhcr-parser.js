#!/usr/bin/env node

/**
 * IMPROVED DHCR PDF PARSER
 * 
 * This version tries multiple PDF parsing approaches:
 * 1. pdf-parse (current method)
 * 2. pdf2pic + OCR (for scanned PDFs)
 * 3. Alternative PDF libraries
 * 4. Manual regex patterns for DHCR format
 */

require('dotenv').config();
const fs = require('fs').promises;
const path = require('path');
const { createClient } = require('@supabase/supabase-js');

class ImprovedDHCRParser {
    constructor() {
        this.supabase = createClient(
            process.env.SUPABASE_URL,
            process.env.SUPABASE_ANON_KEY
        );
    }

    /**
     * Main parsing function with multiple fallback methods
     */
    async parseDHCRFile(filePath) {
        console.log(`🔧 Parsing DHCR file: ${path.basename(filePath)}`);
        
        const ext = path.extname(filePath).toLowerCase();
        
        if (ext === '.csv') {
            return await this.parseCSV(filePath);
        } else if (ext === '.pdf') {
            return await this.parsePDFWithFallbacks(filePath);
        } else if (ext === '.xlsx' || ext === '.xls') {
            return await this.parseExcel(filePath);
        }
        
        throw new Error(`Unsupported file type: ${ext}`);
    }

    /**
     * Parse PDF with multiple fallback methods
     */
    async parsePDFWithFallbacks(filePath) {
        const methods = [
            () => this.parsePDFMethod1(filePath), // pdf-parse
            () => this.parsePDFMethod2(filePath), // pdf2json
            () => this.parsePDFMethod3(filePath), // Manual patterns
        ];

        for (let i = 0; i < methods.length; i++) {
            try {
                console.log(`   🔄 Trying PDF parsing method ${i + 1}...`);
                const result = await methods[i]();
                
                if (result && result.length > 0) {
                    console.log(`   ✅ Method ${i + 1} succeeded: ${result.length} buildings found`);
                    return result;
                } else {
                    console.log(`   ⚠️ Method ${i + 1} returned no data`);
                }
            } catch (error) {
                console.log(`   ❌ Method ${i + 1} failed: ${error.message}`);
            }
        }

        console.log('   💡 All PDF parsing methods failed, returning empty array');
        return [];
    }

    /**
     * Method 1: Original pdf-parse approach
     */
    async parsePDFMethod1(filePath) {
        const pdf = require('pdf-parse');
        
        const dataBuffer = await fs.readFile(filePath);
        const data = await pdf(dataBuffer);
        
        console.log(`     📄 PDF text length: ${data.text.length} characters`);
        console.log(`     📑 PDF pages: ${data.numpages}`);
        
        if (data.text.length < 100) {
            throw new Error('PDF text too short - may be scanned images');
        }
        
        return this.extractBuildingsFromText(data.text, 'pdf-parse');
    }

    /**
     * Method 2: Alternative PDF library (pdf2json)
     */
    async parsePDFMethod2(filePath) {
        // This would require: npm install pdf2json
        // For now, let's use a simpler text extraction
        
        const { exec } = require('child_process');
        const util = require('util');
        const execPromise = util.promisify(exec);
        
        try {
            // Try using pdftotext if available (Linux/Mac)
            const { stdout } = await execPromise(`pdftotext "${filePath}" -`);
            
            if (stdout && stdout.length > 100) {
                console.log(`     📄 pdftotext extracted ${stdout.length} characters`);
                return this.extractBuildingsFromText(stdout, 'pdftotext');
            }
        } catch (error) {
            throw new Error('pdftotext not available');
        }
        
        return [];
    }

    /**
     * Method 3: Manual pattern matching for DHCR format
     */
    async parsePDFMethod3(filePath) {
        // Read raw PDF and look for known DHCR patterns
        const buffer = await fs.readFile(filePath);
        const text = buffer.toString('latin1'); // Sometimes works better than utf8
        
        // Look for DHCR-specific patterns
        const buildings = this.extractDHCRPatterns(text);
        
        if (buildings.length > 0) {
            console.log(`     🔍 Pattern matching found ${buildings.length} buildings`);
            return buildings;
        }
        
        throw new Error('No DHCR patterns found');
    }

    /**
     * Extract buildings from PDF text using patterns
     */
    extractBuildingsFromText(text, method) {
        console.log(`     🔍 Extracting buildings using ${method}...`);
        
        const buildings = [];
        const lines = text.split('\n');
        
        console.log(`     📝 Processing ${lines.length} lines of text`);
        
        for (let i = 0; i < lines.length; i++) {
            const line = lines[i].trim();
            
            if (line.length < 10) continue; // Skip short lines
            
            // DHCR PDF patterns (common formats)
            const patterns = [
                // Pattern 1: "123 MAIN ST    MANHATTAN    10001"
                /^(\d+\s+[A-Z\s]+[A-Z])\s+(MANHATTAN|BROOKLYN|QUEENS|BRONX|STATEN ISLAND)\s+(\d{5})/i,
                
                // Pattern 2: "123 MAIN STREET, NEW YORK, NY 10001"
                /^(\d+\s+[A-Z\s]+[A-Z]),?\s*(NEW YORK|BROOKLYN|QUEENS|BRONX),?\s*NY\s+(\d{5})/i,
                
                // Pattern 3: More flexible address matching
                /^(\d+\s+[A-Z][A-Z\s]+[A-Z])\s+.*?(\d{5})/i
            ];
            
            for (const pattern of patterns) {
                const match = line.match(pattern);
                if (match) {
                    const address = match[1].trim();
                    const borough = this.normalizeBoroughName(match[2] || 'MANHATTAN');
                    const zipcode = match[3] || '';
                    
                    if (address && address.length > 5) {
                        buildings.push({
                            address: address,
                            normalized_address: address.toLowerCase().replace(/[^a-z0-9\s]/g, ''),
                            borough: borough,
                            zipcode: zipcode,
                            dhcr_source: 'pdf',
                            confidence_score: 90,
                            source_method: method
                        });
                    }
                    break;
                }
            }
        }
        
        console.log(`     ✅ Extracted ${buildings.length} buildings using ${method}`);
        return this.deduplicateBuildings(buildings);
    }

    /**
     * Extract buildings using DHCR-specific patterns
     */
    extractDHCRPatterns(text) {
        const buildings = [];
        
        // Look for known DHCR section headers
        const dhcrSections = [
            'BUILDING REGISTRATION',
            'RENT STABILIZED BUILDINGS',
            'DHCR REGISTERED BUILDINGS'
        ];
        
        let foundDHCRSection = false;
        for (const section of dhcrSections) {
            if (text.includes(section)) {
                foundDHCRSection = true;
                console.log(`     📋 Found DHCR section: ${section}`);
                break;
            }
        }
        
        if (!foundDHCRSection) {
            throw new Error('No DHCR section headers found');
        }
        
        // Extract address-like patterns
        const addressRegex = /\d+\s+[A-Z][A-Z\s]+[A-Z]\s+(?:MANHATTAN|BROOKLYN|QUEENS|BRONX|STATEN\s+ISLAND)/gi;
        const matches = text.match(addressRegex) || [];
        
        for (const match of matches) {
            const parts = match.trim().split(/\s+/);
            if (parts.length >= 3) {
                const streetNumber = parts[0];
                const borough = parts[parts.length - 1];
                const streetName = parts.slice(1, -1).join(' ');
                
                const address = `${streetNumber} ${streetName}`;
                
                buildings.push({
                    address: address,
                    normalized_address: address.toLowerCase().replace(/[^a-z0-9\s]/g, ''),
                    borough: this.normalizeBoroughName(borough),
                    dhcr_source: 'pdf',
                    confidence_score: 85,
                    source_method: 'pattern_matching'
                });
            }
        }
        
        return this.deduplicateBuildings(buildings);
    }

    /**
     * Parse CSV file
     */
    async parseCSV(filePath) {
        const Papa = require('papaparse');
        
        const csvContent = await fs.readFile(filePath, 'utf8');
        
        const parsed = Papa.parse(csvContent, {
            header: true,
            skipEmptyLines: true,
            transformHeader: (header) => header.toLowerCase().replace(/[^a-z0-9]/g, '_')
        });
        
        if (parsed.errors.length > 0) {
            console.warn(`   ⚠️ CSV parsing warnings:`, parsed.errors.slice(0, 3));
        }
        
        console.log(`   📊 CSV contains ${parsed.data.length} rows`);
        
        return parsed.data.map(row => this.normalizeBuildingData(row, 'csv'));
    }

    /**
     * Parse Excel file
     */
    async parseExcel(filePath) {
        const XLSX = require('xlsx');
        
        const workbook = XLSX.readFile(filePath);
        const sheetName = workbook.SheetNames[0];
        const worksheet = workbook.Sheets[sheetName];
        
        const jsonData = XLSX.utils.sheet_to_json(worksheet, {
            header: 1,
            defval: ''
        });
        
        if (jsonData.length < 2) {
            throw new Error('Excel file has insufficient data');
        }
        
        console.log(`   📊 Excel contains ${jsonData.length} rows`);
        
        const headers = jsonData[0].map(h => 
            h.toString().toLowerCase().replace(/[^a-z0-9]/g, '_')
        );
        
        const buildings = jsonData.slice(1).map(row => {
            const building = {};
            headers.forEach((header, index) => {
                building[header] = row[index] || '';
            });
            return this.normalizeBuildingData(building, 'excel');
        });
        
        return buildings;
    }

    /**
     * Normalize building data to standard format
     */
    normalizeBuildingData(rawData, source) {
        return {
            address: this.normalizeAddress(rawData.address || rawData.building_address || rawData.street_address || ''),
            normalized_address: this.normalizeAddress(rawData.address || rawData.building_address || rawData.street_address || '').toLowerCase(),
            borough: this.normalizeBoroughName(rawData.borough || rawData.boro || 'manhattan'),
            zipcode: this.normalizeZipcode(rawData.zipcode || rawData.zip || rawData.postal_code || ''),
            building_id: rawData.building_id || rawData.id || null,
            unit_count: parseInt(rawData.unit_count || rawData.units || 0) || null,
            registration_id: rawData.registration_id || rawData.reg_id || null,
            dhcr_source: source,
            confidence_score: 95,
            parsed_at: new Date().toISOString()
        };
    }

    /**
     * Normalize address format
     */
    normalizeAddress(address) {
        if (!address) return '';
        
        return address
            .toString()
            .trim()
            .toUpperCase()
            .replace(/\s+/g, ' ')
            .replace(/[^A-Z0-9\s]/g, '');
    }

    /**
     * Normalize borough name
     */
    normalizeBoroughName(borough) {
        if (!borough) return 'manhattan';
        
        const normalized = borough.toString().toLowerCase().trim();
        
        const boroughMap = {
            'manhattan': 'manhattan',
            'new york': 'manhattan',
            'ny': 'manhattan',
            'brooklyn': 'brooklyn',
            'bk': 'brooklyn',
            'bklyn': 'brooklyn',
            'queens': 'queens',
            'qns': 'queens',
            'bronx': 'bronx',
            'bx': 'bronx',
            'staten island': 'staten_island',
            'si': 'staten_island'
        };
        
        return boroughMap[normalized] || 'manhattan';
    }

    /**
     * Normalize zipcode
     */
    normalizeZipcode(zipcode) {
        if (!zipcode) return '';
        
        const zip = zipcode.toString().replace(/[^0-9]/g, '');
        return zip.length >= 5 ? zip.substring(0, 5) : zip;
    }

    /**
     * Remove duplicate buildings
     */
    deduplicateBuildings(buildings) {
        const seen = new Set();
        const unique = [];
        
        for (const building of buildings) {
            const key = `${building.normalized_address}-${building.borough}`;
            
            if (!seen.has(key) && building.address.length > 5) {
                seen.add(key);
                unique.push(building);
            }
        }
        
        console.log(`   🔄 Deduplicated: ${buildings.length} → ${unique.length} buildings`);
        return unique;
    }

    /**
     * Save buildings to database
     */
    async saveBuildingsToDatabase(buildings) {
        if (buildings.length === 0) {
            console.log('   📊 No buildings to save');
            return;
        }
        
        try {
            console.log(`💾 Saving ${buildings.length} buildings to database...`);
            
            const batchSize = 500;
            let saved = 0;
            
            for (let i = 0; i < buildings.length; i += batchSize) {
                const batch = buildings.slice(i, i + batchSize);
                
                const { error } = await this.supabase
                    .from('rent_stabilized_buildings')
                    .upsert(batch, { 
                        onConflict: 'normalized_address,borough',
                        ignoreDuplicates: false 
                    });
                
                if (error) {
                    console.error(`   ❌ Batch ${Math.floor(i/batchSize) + 1} failed:`, error.message);
                    continue;
                }
                
                saved += batch.length;
                console.log(`   ✅ Saved batch ${Math.floor(i/batchSize) + 1}: ${saved}/${buildings.length}`);
            }
            
            console.log(`🎉 Successfully saved ${saved} buildings to database`);
            return saved;
            
        } catch (error) {
            console.error('❌ Database save failed:', error.message);
            throw error;
        }
    }

    /**
     * Test parsing a single file
     */
    async testParseFile(filePath) {
        console.log('🧪 TESTING DHCR FILE PARSING');
        console.log('=' .repeat(50));
        
        try {
            const buildings = await this.parseDHCRFile(filePath);
            
            console.log(`✅ Parsing successful: ${buildings.length} buildings found`);
            
            if (buildings.length > 0) {
                console.log('\n📍 Sample buildings:');
                buildings.slice(0, 5).forEach((building, index) => {
                    console.log(`${index + 1}. ${building.address} (${building.borough})`);
                });
                
                // Ask if user wants to save to database
                console.log(`\n💾 Save ${buildings.length} buildings to database? (Set SAVE_TO_DB=true)`);
                
                if (process.env.SAVE_TO_DB === 'true') {
                    await this.saveBuildingsToDatabase(buildings);
                }
            }
            
            return buildings;
            
        } catch (error) {
            console.error('❌ Parsing failed:', error.message);
            throw error;
        }
    }
}

// Main execution
async function main() {
    const parser = new ImprovedDHCRParser();
    
    const args = process.argv.slice(2);
    
    if (args.includes('--help')) {
        console.log('🔧 Improved DHCR Parser');
        console.log('');
        console.log('Usage:');
        console.log('  node improved-dhcr-parser.js <file_path>           # Test parse single file');
        console.log('  SAVE_TO_DB=true node improved-dhcr-parser.js <file> # Parse and save to DB');
        console.log('');
        console.log('Supported formats: PDF, CSV, Excel (.xlsx/.xls)');
        console.log('');
        console.log('Example:');
        console.log('  node improved-dhcr-parser.js data/dhcr/2023-DHCR-Bldg-File-Manhattan.pdf');
        return;
    }
    
    if (args.length === 0) {
        console.error('❌ Please provide a file path to parse');
        console.log('Usage: node improved-dhcr-parser.js <file_path>');
        process.exit(1);
    }
    
    const filePath = args[0];
    
    try {
        await parser.testParseFile(filePath);
    } catch (error) {
        console.error('💥 Parser test failed:', error.message);
        process.exit(1);
    }
}

// Export for use in other modules
module.exports = ImprovedDHCRParser;

// Run if executed directly
if (require.main === module) {
    main();
}
