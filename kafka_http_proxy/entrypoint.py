#!/usr/bin/env python3
import sys
import os

def main():
    mode = os.getenv('MODE', 'proxy')
    
    if mode == 'proxy':
        from kafka_http_proxy.proxy.service import main as proxy_main
        sys.exit(proxy_main())
    elif mode == 'processor':
        from kafka_http_proxy.processor.service import main as processor_main
        sys.exit(processor_main())
    else:
        print(f"Unknown MODE: {mode}. Use 'proxy' or 'processor'")
        sys.exit(1)

if __name__ == "__main__":
    main()