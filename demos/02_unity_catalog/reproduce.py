"""Demo 2c: the pin is what makes a re-run a re-run.

    python governed_read.py run          # run 1, pins the table at vN
    python mutate_orders.py run          # table moves to vN+1
    python reproduce.py run              # reads vN anyway, and shows the difference

By default this picks up the latest successful `GovernedReadFlow`; pass
`--origin-run <run_id>` to replay a specific one. Both reads go through credential
vending, so no warehouse or cluster is involved.
"""

from _env import step_env

from metaflow import Flow, FlowSpec, Parameter, Run, step


class ReproduceFlow(FlowSpec):
    origin_run = Parameter("origin-run", default=None, type=str)

    @step_env()
    @step
    def start(self):
        origin = (
            Run("GovernedReadFlow/%s" % self.origin_run)
            if self.origin_run
            else Flow("GovernedReadFlow").latest_successful_run
        )
        print("replaying %s" % origin.pathspec)

        # The artifact carries the pin, so nothing here has to know which version to ask
        # for: reproducibility is a property of the run, not of a number someone
        # remembered to write down.
        self.orders = origin["start"].task.data.orders
        print("as seen by that run: %r" % self.orders)
        self.next(self.compare)

    @step_env("vending")
    @step
    def compare(self):
        latest = self.orders.latest()
        self.pinned_version = self.orders.version
        self.latest_version = latest.version
        self.pinned_rows = self.orders.to_arrow(columns=["order_id"]).num_rows
        self.latest_rows = latest.to_arrow(columns=["order_id"]).num_rows
        self.next(self.end)

    @step_env()
    @step
    def end(self):
        def fmt(version):
            return "v%s" % version if version is not None else "unpinned"

        print(
            "pinned %s: %d rows\nlatest %s: %d rows"
            % (
                fmt(self.pinned_version),
                self.pinned_rows,
                fmt(self.latest_version),
                self.latest_rows,
            )
        )
        if self.pinned_version is None:
            print(
                "\nThe pin did not take at assignment time (see the warning on the "
                "`start` step of the original run), so both reads saw the current table."
            )
        elif self.pinned_rows == self.latest_rows:
            print("\nSame counts, so run mutate_orders.py and try again.")
        else:
            print(
                "\nThe table moved and the earlier run did not. Delta time travel does "
                "the work; Metaflow only has to remember the version."
            )


if __name__ == "__main__":
    ReproduceFlow()
