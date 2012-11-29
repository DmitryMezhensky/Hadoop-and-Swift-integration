package com.mirantis.swift.fs;

import java.io.FileNotFoundException;
import java.io.PrintWriter;

/**
 * @author dmezhensky
 */
public class App {
    public static void main(String[] args) throws FileNotFoundException {
        String filename = "object";

        for (int i = 0; i < 7; i++) {
            final PrintWriter printWriter = new PrintWriter(filename + String.valueOf(i + 4));
            for (int j = 1; j < 5000000; ++j) {
                printWriter.write("file".concat(String.valueOf(i + 4)) + " line".concat(String.valueOf(j)));
                printWriter.write("\n");
            }
            printWriter.close();
        }
    }
}
