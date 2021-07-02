package com.kwai.sod;

public class NativeRun {
    private static native byte[] callNative(byte[] stageDescription, String inputPaths, String outputDir, String outputName);

    static {
        System.loadLibrary("com_kwai_sod_lib");
    }

    public static void main(String[] args) {
        byte[] taskResult = callNative(new byte[0], "a", "1", "2");
        System.out.println(taskResult);
    }
}

