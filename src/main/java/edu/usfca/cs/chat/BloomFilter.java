package edu.usfca.cs.chat;

import com.sangupta.murmur.Murmur3;

import java.util.ArrayList;
import java.util.BitSet;

public class BloomFilter {
    private int m; // number of bits
    private int k; // number of hash funcs
    private int n; // number of elements
    private BitSet bits;
    private double E = 2.71828;

    public BloomFilter(int m, int k) {
        this.m = m;
        this.k = k;
        this.bits = new BitSet(m);
        this.n = 0;
    }

    private byte[] longToBytes(long x) {
        byte[] result = new byte[8];
        for (int i = 7; i >= 0; i--) {
            result[i] = (byte)(x & 0xFF);
            x >>= 8;
        }
        return result;
    }

    private ArrayList<Integer> getHashedBits(byte[] data) {
        ArrayList<Integer> bitsList = new ArrayList<>();
        int i, idx;
        long hash2;

        long hash1 = Murmur3.hash_x86_32(data, data.length, 0);
        byte[] hashedData = longToBytes(hash1);

        for(i = 0; i < k; i++) {
            hash2 = Murmur3.hash_x86_32(hashedData, hashedData.length, 0);
            idx = (int) (hash2 % m);
            bitsList.add(idx);
            hashedData = longToBytes(hash2);
        }

        return bitsList;
    }

    private ArrayList<Integer> getOptHashedBits(byte[] data) {
        int idx;
        ArrayList<Integer> bitsList = new ArrayList<>();

        // Create two 32-bit hashes:
        long hash1 = Murmur3.hash_x86_32(data, data.length, 0);
        long hash2 = Murmur3.hash_x86_32(data, data.length, hash1);

        for(int i = 0 ; i < k ;i++){
            idx = (int)((hash1 + i * hash2) % m);
            bitsList.add(idx);
        }
        return bitsList;
    }

    void put(byte[] data) {
        ArrayList<Integer> indexes = getHashedBits(data);
        indexes.forEach(idx -> bits.set(idx));
        n++; // increment total elements
    }

    void putOpt(byte[] data) {
        ArrayList<Integer> indexes = getOptHashedBits(data);
        indexes.forEach(idx -> bits.set(idx));
        n++; // increment total elements
    }
    boolean get(byte[] data) {
        ArrayList<Integer> indexes = getHashedBits(data);
        for(Integer i : indexes) {
            if(!bits.get(i)) return false;
        }

        return true;
    }

    boolean getOpt(byte[] data) {
        ArrayList<Integer> indexes = getOptHashedBits(data);
        for(Integer i : indexes) {
            if(!bits.get(i)) return false;
        }
        return true;
    }

    // formula: (1 - (e)^((k*n)/m)*-1))^k
    float falsePositiveProb() { ;
        float powerOfE = (float)((k * n) / m ) * -1;
        return (float) Math.pow((1 - Math.pow(E, powerOfE)), k);
    }

    public static void main(String[] args) {
        BloomFilter bf = new BloomFilter(20, 3);

        long a = 1000;

        String testFile = "/Users/Desktop/Videos/MyQuar.mov";
        byte[] data = testFile.getBytes();

        System.out.println("data: " + data);

        System.out.println("bits len: " + bf.bits.length());

        bf.put(data);

        System.out.println("bits len: " + bf.bits.length());

        System.out.println(bf.get(data));
//
//        long currTime = System.currentTimeMillis();
//        for(int i = 0; i < 100; i ++) {
//            a += i;
//            byte[] arr = bf.longToBytes(a);
//            bf.put(arr);
//            System.out.println(bf.get(arr));
////            System.out.println(bf.bits.length());
////            System.out.println(bf.falsePositiveProb());
//            System.out.println("****************");
//        }
//        long normalTime = System.currentTimeMillis() - currTime;
//
//        currTime = System.currentTimeMillis();
//        for(int i = 0; i < 100; i ++) {
//            a += i;
//            byte[] arr = bf.longToBytes(a);
//            bf.putOpt(arr);
//            System.out.println(bf.getOpt(arr));
////            System.out.println(bf.bits.length());
////            System.out.println(bf.falsePositiveProb());
//            System.out.println("****************");
//        }
//        long optTime = System.currentTimeMillis() - currTime;
//
//        System.out.println("normalTime = " + normalTime);
//        System.out.println("optTime = " + optTime);
    }
}
