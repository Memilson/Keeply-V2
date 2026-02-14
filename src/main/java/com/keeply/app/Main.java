package com.keeply.app;

import java.util.Arrays;

import com.keeply.app.api.KeeplyApi;
import com.keeply.app.cli.cli;

public final class Main {

    private Main() {}

    public static void main(String[] args) {
        if (args != null && args.length > 0 && "api".equalsIgnoreCase(args[0])) {
            KeeplyApi.run(Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        cli.run(args);
    }
}
