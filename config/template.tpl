{ {{range $i,$v := service "ambari_server"}}
    "ip": [{{ if ne $i 0 }}, {{ end }}"{{ $v.Address }}"],
    "port": [{{ if ne $i 0 }}, {{ end }}"{{ $v.Port }}"]{{ end }},
    "proxy_port": {{ with $d := key "nginx_http_proxy/ambari_server/ambari_server" | parseJSON }}"{{ $d.proxy_port }}"{{ end }}
}