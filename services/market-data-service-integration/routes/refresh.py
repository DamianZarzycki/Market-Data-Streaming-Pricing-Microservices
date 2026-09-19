def register(app, poll_workers):
    @app.route("/market-data/refresh")
    def refresh():
        return poll_workers().trigger_now()
