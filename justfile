default :
    just --list
    
run module : 
    RUST_BACKTRACE=true RUST_LOG=debug cargo run --example {{module}}

cert : 
    mkdir -p cert
    mkcert -install
    mkcert -ecdsa -cert-file cert/cert.pem -key-file cert/key.pem localhost 127.0.0.1 ::1 dinesh-macmini dinesh-macbook
    openssl x509 -in cert/cert.pem -outform der -out cert/cert.der
    openssl ec -in cert/key.pem -outform der -out cert/key.der
