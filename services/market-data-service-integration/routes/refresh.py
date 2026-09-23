def register(app, poll_workers):
    # POST is the documented verb (an immediate, side-effecting fetch); GET is
    # kept too so the existing curl smoke tests keep working.
    @app.route("/market-data/refresh", method=["GET", "POST"])
    def refresh():
        return poll_workers().run_quote_cycle()

    @app.route("/curves/refresh", method=["GET", "POST"])
    def refresh_curves():
        return poll_workers().run_curve_cycle()
