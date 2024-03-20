package org.urbcomp.startdb.compress.elf;

import com.github.kutschkem.fpc.FpcCompressor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.compress.brotli.BrotliCodec;
import org.apache.hadoop.hbase.io.compress.lz4.Lz4Codec;
import org.apache.hadoop.hbase.io.compress.xerial.SnappyCodec;
import org.apache.hadoop.hbase.io.compress.xz.LzmaCodec;
import org.apache.hadoop.hbase.io.compress.zstd.ZstdCodec;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.urbcomp.startdb.compress.elf.compressor.*;
import org.urbcomp.startdb.compress.elf.decompressor.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

public class MyCompression {
    private static final String FILE_PATH = "~/prj/Bachelorarbeit/Datasets";
    /*private static final String[] FILENAMES = {
            "/init.csv",    //First run a dataset to ensure the relevant hbase settings of the zstd and snappy compressors
            "/Air-pressure.csv",
            "/Air-sensor.csv",
            "/Basel-temp.csv",
            "/Basel-wind.csv",
            "/Bird-migration.csv",
            "/Bitcoin-price.csv",
            "/Blockchain-tr.csv",
            "/City-temp.csv",
            "/City-lat.csv",
            "/City-lon.csv",
            "/Dew-point-temp.csv",
            "/electric_vehicle_charging.csv",
            "/Food-price.csv",
            "/IR-bio-temp.csv",
            "/PM10-dust.csv",
            "/SSD-bench.csv",
            "/POI-lat.csv",
            "/POI-lon.csv",
            "/Stocks-DE.csv",
            "/Stocks-UK.csv",
            "/Stocks-USA.csv",
            "/Wind-Speed.csv",
    };*/
    private static final String STORE_RESULT = "~/prj/Bachelorarbeit/results";

    private static final double TIME_PRECISION = 1000.0;
    List<Map<String, ResultStructure>> allResult = new ArrayList<>();

    public void MyCompression(String compressor, String filename) throws IOException {
        compressor = compressor.toLowerCase();
        Map<String, List<ResultStructure>> result = new HashMap<>();
        testCompressor(filename, compressor, result);
        for (Map.Entry<String, List<ResultStructure>> kv : result.entrySet()) {
            Map<String, ResultStructure> r = new HashMap<>();
            r.put(kv.getKey(), computeAvg(kv.getValue()));
            allResult.add(r);
        }
        if (result.isEmpty()) {
            System.out.println("The result of the file " + filename +
                    " is empty because the amount of data is less than one block, and the default is at least 1000.");
        }
        storeResult(compressor);
    }


    private void testCompressor(String fileName, String compressor, Map<String, List<ResultStructure>> resultCompressor) throws FileNotFoundException {
        FileReader fileReader = new FileReader(FILE_PATH + fileName);

        float totalBlocks = 0;
        double[] values;

        HashMap<String, List<Double>> totalCompressionTime = new HashMap<>();
        HashMap<String, List<Double>> totalDecompressionTime = new HashMap<>();
        HashMap<String, Long> key2TotalSize = new HashMap<>();

        while ((values = fileReader.nextBlock()) != null) {
            totalBlocks += 1;
            /*
            ICompressor[] compressors = new ICompressor[]{
                    new GorillaCompressorOS(),
                    new ElfOnGorillaCompressorOS(),
                    new ChimpCompressor(),
                    new ElfOnChimpCompressor(),
                    new ChimpNCompressor(128),
                    new ElfOnChimpNCompressor(128),
                    new ElfCompressor(),
            };*/
            ICompressor compressorToUse;
            switch (compressor) {
                case "elf":
                    compressorToUse = new ElfCompressor();
                    break;
                case "chimp":
                    compressorToUse = new ChimpCompressor();
                    break;
                case "gorilla":
                    compressorToUse = new GorillaCompressorOS();
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + compressor);
            }

            double encodingDuration;
            double decodingDuration;
            long start = System.nanoTime();
            for (double value : values) {
                compressorToUse.addValue(value);
            }
            compressorToUse.close();

            encodingDuration = System.nanoTime() - start;

            byte[] result = compressorToUse.getBytes();

            IDecompressor decompressorToUse;
            switch (compressor) {
                case "elf":
                    decompressorToUse = new ElfDecompressor(result);
                    break;
                case "chimp":
                    decompressorToUse = new ChimpDecompressor(result);
                    break;
                case "gorilla":
                    decompressorToUse = new GorillaDecompressorOS(result);
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + compressor);
            }

            IDecompressor[] decompressors = new IDecompressor[]{
                    new GorillaDecompressorOS(result),
                    new ElfOnGorillaDecompressorOS(result),
                    new ChimpDecompressor(result),
                    new ElfOnChimpDecompressor(result),
                    new ChimpNDecompressor(result, 128),
                    new ElfOnChimpNDecompressor(result, 128),
                    new ElfDecompressor(result)
            };

            start = System.nanoTime();
            List<Double> uncompressedValues = decompressorToUse.decompress();
            decodingDuration = System.nanoTime() - start;

            for (int j = 0; j < values.length; j++) {
                if (values[j] != uncompressedValues.get(j)) {
                    throw new RuntimeException("Value did not match" + compressorToUse.getKey());
                }
            }

            String key = compressorToUse.getKey();
            if (!totalCompressionTime.containsKey(key)) {
                totalCompressionTime.put(key, new ArrayList<>());
                totalDecompressionTime.put(key, new ArrayList<>());
                key2TotalSize.put(key, 0L);
            }
            totalCompressionTime.get(key).add(encodingDuration / TIME_PRECISION);
            totalDecompressionTime.get(key).add(decodingDuration / TIME_PRECISION);
            key2TotalSize.put(key, compressorToUse.getSize() + key2TotalSize.get(key));

        }

        for (Map.Entry<String, Long> kv : key2TotalSize.entrySet()) {
            String key = kv.getKey();
            Long totalSize = kv.getValue();
            ResultStructure r = new ResultStructure(fileName, key,
                    totalSize / (totalBlocks * FileReader.DEFAULT_BLOCK_SIZE * 64.0),
                    totalCompressionTime.get(key),
                    totalDecompressionTime.get(key)
            );
            if (!resultCompressor.containsKey(key)) {
                resultCompressor.put(key, new ArrayList<>());
            }
            resultCompressor.get(key).add(r);
        }
    }

    private void storeResult(String location) throws IOException {
        String filePath = STORE_RESULT + "/" + location;
        File file = new File(filePath).getParentFile();
        if (!file.exists() && !file.mkdirs()) {
            throw new IOException("Create directory failed: " + file);
        }
        try (FileWriter fileWriter = new FileWriter(filePath)) {
            fileWriter.write(ResultStructure.getHead());
            for (Map<String, ResultStructure> result : allResult) {
                for (ResultStructure ls : result.values()) {
                    fileWriter.write(ls.toString());
                }
            }
        }
    }

    private ResultStructure computeAvg(List<ResultStructure> lr) {
        int num = lr.size();
        double compressionTime = 0;
        double maxCompressTime = 0;
        double minCompressTime = 0;
        double mediaCompressTime = 0;
        double decompressionTime = 0;
        double maxDecompressTime = 0;
        double minDecompressTime = 0;
        double mediaDecompressTime = 0;
        for (ResultStructure resultStructure : lr) {
            compressionTime += resultStructure.getCompressionTime();
            maxCompressTime += resultStructure.getMaxCompressTime();
            minCompressTime += resultStructure.getMinCompressTime();
            mediaCompressTime += resultStructure.getMediaCompressTime();
            decompressionTime += resultStructure.getDecompressionTime();
            maxDecompressTime += resultStructure.getMaxDecompressTime();
            minDecompressTime += resultStructure.getMinDecompressTime();
            mediaDecompressTime += resultStructure.getMediaDecompressTime();
        }
        return new ResultStructure(lr.get(0).getFilename(),
                lr.get(0).getCompressorName(),
                lr.get(0).getCompressorRatio(),
                compressionTime / num,
                maxCompressTime / num,
                minCompressTime / num,
                mediaCompressTime / num,
                decompressionTime / num,
                maxDecompressTime / num,
                minDecompressTime / num,
                mediaDecompressTime / num
        );
    }

    private static double[] toDoubleArray(byte[] byteArray) {
        int times = Double.SIZE / Byte.SIZE;
        double[] doubles = new double[byteArray.length / times];
        for (int i = 0; i < doubles.length; i++) {
            doubles[i] = ByteBuffer.wrap(byteArray, i * times, times).getDouble();
        }
        return doubles;
    }
}

