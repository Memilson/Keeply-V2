import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/api/auth": {
        target: "http://localhost:18081",
        changeOrigin: true,
      },
      "/api/keeply-ws": {
        target: "http://localhost:8092",
        changeOrigin: true,
      },
    },
  },
});
