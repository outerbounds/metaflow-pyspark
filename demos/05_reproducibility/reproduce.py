"""Chapter 5c: the pin is what makes a re-run a re-run.

    python governed_read.py run          # pins the table at vN
    python mutate_orders.py run          # the table moves to vN+1
    python reproduce.py run              # reads vN anyway, and shows the difference

Picks up the latest successful `GovernedReadFlow`; pass `--origin-run <run_id>` to
replay a specific one. Both reads go through credential vending, so comparing two
versions of the table starts no compute on Databricks.
"""

from _env import step_env
from _orders import DAILY_SQL, same_answer

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
        # The artifact carries the pin, so nothing here has to know which version to
        # ask for.
        self.orders = origin["start"].task.data.orders
        self.original_daily = origin["summarize"].task.data.daily
        self.next(self.compare)

    @step_env("vending")
    @step
    def compare(self):
        latest = self.orders.latest()
        self.pinned_version, self.latest_version = self.orders.version, latest.version
        self.pinned_rows = self.orders.to_arrow().num_rows
        self.latest_rows = latest.to_arrow().num_rows

        connection = self.orders.to_duckdb(view_name="orders")
        replayed = connection.execute(DAILY_SQL.format(source="orders")).df()
        self.reproduced = same_answer(self.original_daily, replayed)
        self.next(self.end)

    @step_env()
    @step
    def end(self):
        print("pinned v%s: %d rows" % (self.pinned_version, self.pinned_rows))
        print("latest v%s: %d rows" % (self.latest_version, self.latest_rows))
        if self.pinned_version is None:
            print(
                "\nThe original run could not pin a version (see the warning on its "
                "start step), so there is nothing to replay."
            )
        elif self.pinned_rows == self.latest_rows:
            print("\nSame counts, so run mutate_orders.py and try again.")
        else:
            print(
                "\nThe table moved and the replay did not: the daily summary "
                "recomputed from the pin %s the original run's."
                % ("matches" if self.reproduced else "DOES NOT match")
            )


if __name__ == "__main__":
    ReproduceFlow()
