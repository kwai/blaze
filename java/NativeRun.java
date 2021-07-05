package com.kwai.sod;

public class NativeRun {
    private static native byte[] callNative(byte[] task, String executorId, String workDir, String fileName);

    static {
        System.loadLibrary("com_kwai_sod_lib");
    }

    public static void main(String[] args) {
        byte[] taskResult = callNative(new byte[0], "exec1", "1", "2");
        System.out.println(taskResult);
    }
}
