{application, chttpd, [
    {description, "HTTP interface for CouchDB cluster"},
    {vsn, "1.0.3"},
    {modules, [
        chttpd,
        chttpd_app,
        chttpd_auth,
        chttpd_db,
        chttpd_external,
        chttpd_misc,
        chttpd_oauth,
        chttpd_rewrite,
        chttpd_show,
        chttpd_sup,
        chttpd_view
    ]},
    {registered, [chttpd_sup, chttpd]},
    {applications, [kernel, stdlib, couch, fabric]},
    {mod, {chttpd_app,[]}}
]}.