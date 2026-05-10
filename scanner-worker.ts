import { parentPort, workerData } from 'worker_threads';
import * as bitcoin from 'bitcoinjs-lib';
import * as ecc from 'tiny-secp256k1';
import { ECPairFactory } from 'ecpair';
import { Wallet } from 'ethers';
import { Keypair } from '@solana/web3.js';
import { randomBytes } from 'crypto';

// Initialize ECC for Taproot support
bitcoin.initEccLib(ecc);
const ECPair = ECPairFactory(ecc);

const { targets, startHex, endHex, random, network, threadIndex = 0, totalThreads = 1, acceleration = "CPU" } = workerData;
const targetSet = new Set(targets);

// --- GPU/CUDA Optimization Placeholder ---
// When running locally with CUDA, you would use a native binding here.
// For example: const cuda = require('node-cuda');
// Or call a compiled bitcrack binary.
const isGPUMode = acceleration === "GPU";

function safeBigInt(val: string | number | bigint): bigint {
  if (typeof val === 'bigint') return val;
  if (typeof val === 'number') return BigInt(val);
  const clean = val.trim().toLowerCase();
  if (clean.startsWith('0x')) return BigInt(clean);
  if (/[a-f]/.test(clean) || clean.length > 16) {
    return BigInt('0x' + clean);
  }
  return BigInt(clean);
}

const start = safeBigInt(startHex);
const end = safeBigInt(endHex);
const range = end - start;
let current = random ? start : start + BigInt(threadIndex);
let totalScanned = 0;
let lastUpdate = Date.now();
let kps = 0;

function getRandomBigInt(min: bigint, max: bigint): bigint {
  const range = max - min;
  if (range <= 0n) return min;
  
  const byteLength = (range.toString(16).length + 1) / 2;
  const buffer = randomBytes(Math.ceil(byteLength) + 8); // Add extra bytes to reduce bias
  let rand = 0n;
  for (let i = 0; i < buffer.length; i++) {
    rand = (rand << 8n) + BigInt(buffer[i]);
  }
  
  return (rand % (range + 1n)) + min;
}

interface AddressResult {
  address: string;
  type: string;
}

function getAddresses(privKeyBigInt: bigint, net: string): AddressResult[] {
  const hex = privKeyBigInt.toString(16).padStart(64, '0');
  const privKeyBuffer = Buffer.from(hex, 'hex');
  
  try {
    const results: AddressResult[] = [];

    if (net === 'BTC') {
      // Compressed KeyPair
      const keyPair = ECPair.fromPrivateKey(privKeyBuffer, { compressed: true });
      const pubkey = keyPair.publicKey;
      
      // Uncompressed KeyPair
      const keyPairUncompressed = ECPair.fromPrivateKey(privKeyBuffer, { compressed: false });
      const pubkeyUncompressed = keyPairUncompressed.publicKey;
      
      // 1. Legacy (P2PKH) - Compressed
      const p2pkh = bitcoin.payments.p2pkh({ pubkey });
      if (p2pkh.address) results.push({ address: p2pkh.address, type: 'LEGACY' });
      
      // 2. Legacy (P2PKH) - Uncompressed
      const p2pkhU = bitcoin.payments.p2pkh({ pubkey: pubkeyUncompressed });
      if (p2pkhU.address) results.push({ address: p2pkhU.address, type: 'LEGACY (U)' });
      
      // 3. Native SegWit (P2WPKH)
      const p2wpkh = bitcoin.payments.p2wpkh({ pubkey });
      if (p2wpkh.address) results.push({ address: p2wpkh.address, type: 'SEGWIT' });
      
      // 4. Nested SegWit (P2SH-P2WPKH)
      const p2sh = bitcoin.payments.p2sh({ redeem: p2wpkh });
      if (p2sh.address) results.push({ address: p2sh.address, type: 'P2SH' });
      
      // 5. Taproot (P2TR)
      const internalPubkey = pubkey.slice(1, 33);
      const p2tr = bitcoin.payments.p2tr({ internalPubkey });
      if (p2tr.address) results.push({ address: p2tr.address, type: 'TAPROOT' });

      // 6. EVM (Ethereum)
      const wallet = new Wallet('0x' + hex);
      results.push({ address: wallet.address, type: 'EVM' });
      
      return results;
    } else if (net === 'ETH') {
      const wallet = new Wallet('0x' + hex);
      return [{ address: wallet.address.toLowerCase(), type: 'EVM' }];
    } else if (net === 'SOL') {
      const keypair = Keypair.fromSeed(privKeyBuffer);
      return [{ address: keypair.publicKey.toBase58(), type: 'SOLANA' }];
    }
    return [];
  } catch (e) {
    return [];
  }
}

function scan() {
  const startTime = Date.now();
  // Increase batch size for GPU mode if it were implemented
  let batchSize = isGPUMode ? 5000 : (network === 'BTC' ? 250 : 500); 
  
  while (true) {
    for (let i = 0; i < batchSize; i++) {
      let privKey: bigint;
      if (random) {
        privKey = getRandomBigInt(start, end);
      } else {
        privKey = current;
        current += BigInt(totalThreads);
      }

      const results = getAddresses(privKey, network);
      
      // Send update with sample address
      const now = Date.now();
      if (now - lastUpdate > 1000) {
        kps = Math.floor((totalScanned / (now - startTime)) * 1000);
        const progress = random ? 0 : (end > start ? Number((current - start) * 10000n / (end - start)) / 100 : 0);
        
        parentPort?.postMessage({
          type: 'update',
          currentKey: privKey.toString(16).padStart(64, '0'),
          sampleAddresses: results.map(r => r.address),
          kps,
          total: totalScanned,
          progress
        });
        lastUpdate = now;
      }

      const hex = privKey.toString(16).padStart(64, '0');
      const privKeyBuffer = Buffer.from(hex, 'hex');

      for (const res of results) {
        if (res.address && (targetSet.has(res.address) || targetSet.has(res.address.toLowerCase()))) {
          let wif = '';
          let wifU = '';
          
          if (network === 'BTC') {
            try {
              const keyPair = ECPair.fromPrivateKey(privKeyBuffer, { compressed: true });
              const keyPairU = ECPair.fromPrivateKey(privKeyBuffer, { compressed: false });
              wif = keyPair.toWIF();
              wifU = keyPairU.toWIF();
            } catch(e) {}
          }

          parentPort?.postMessage({
            type: 'found',
            data: {
              address: res.address,
              privateKey: hex,
              wif,
              wifU,
              timestamp: new Date().toISOString(),
              network,
              walletType: res.type
            }
          });
        }
      }

      totalScanned++;
      
      if (current > end && !random) {
        parentPort?.postMessage({ type: 'done' });
        return;
      }
    }
  }
}

scan();
