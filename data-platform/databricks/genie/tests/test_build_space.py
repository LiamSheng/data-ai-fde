"""The Genie space definition generated from instructions.md and benchmarks.yml."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from build_space import BenchmarkError, build_space


def node(name, schema, resource_type="model"):
    return {
        "name": name,
        "resource_type": resource_type,
        "database": "workspace",
        "schema": schema,
        "relation_name": f"`workspace`.`{schema}`.`{name}`",
    }


MANIFEST = {
    "nodes": {
        "model.t.fct_trip_delay_sample": node(
            "fct_trip_delay_sample", "transit_dev_gold"
        ),
        "model.t.dim_routes": node("dim_routes", "transit_dev_silver"),
        "seed.t.route_aliases": node("route_aliases", "transit_dev_silver", "seed"),
        "model.t.route_vehicle_counts": node(
            "route_vehicle_counts", "transit_dev_gold"
        ),
    }
}
FUNCTION = "workspace.transit_dev_gold.route_delay_summary"


def call(route="99", day_type="weekday", start_hour=15, end_hour=18):
    return {
        "route": route,
        "day_type": day_type,
        "start_hour": start_hour,
        "end_hour": end_hour,
    }


def answers(space):
    """{question: SQL answer} of the space's benchmarks."""
    return {
        "".join(q["question"]): "".join(q["answer"][0]["content"])
        for q in space["benchmarks"]["questions"]
    }


class BenchmarkAnswers(unittest.TestCase):
    def test_answer_calls_the_deployed_function_with_literal_arguments(self):
        space = build_space(
            MANIFEST,
            "rules",
            {"questions": [{"phrasings": ["How late is the 99?"], "calls": [call()]}]},
        )

        self.assertEqual(
            answers(space),
            {
                "How late is the 99?": f"SELECT * FROM {FUNCTION}('99', 'weekday', 15, 18)"
            },
        )
        self.assertEqual(
            space["benchmarks"]["questions"][0]["answer"][0]["format"], "SQL"
        )

    def test_comparison_unions_its_calls_and_every_phrasing_shares_the_answer(self):
        space = build_space(
            MANIFEST,
            "rules",
            {
                "questions": [
                    {
                        "phrasings": ["99 weekday vs saturday?", "99 工作日和周六比？"],
                        "calls": [call(), call(day_type="saturday")],
                    }
                ]
            },
        )

        expected = (
            f"SELECT * FROM {FUNCTION}('99', 'weekday', 15, 18) UNION ALL "
            f"SELECT * FROM {FUNCTION}('99', 'saturday', 15, 18)"
        )
        self.assertEqual(
            answers(space),
            {"99 weekday vs saturday?": expected, "99 工作日和周六比？": expected},
        )

    def test_rejects_calls_the_function_would_answer_wrongly(self):
        bad_calls = {
            "unknown day type": call(day_type="holiday"),
            "empty window": call(start_hour=18, end_hour=18),
            "hour past midnight": call(start_hour=22, end_hour=25),
            "hour as text": call(start_hour="15"),
            "missing argument": {
                "route": "99",
                "day_type": "weekday",
                "start_hour": 15,
            },
            "blank route": call(route=" "),
        }
        for reason, bad in bad_calls.items():
            with self.subTest(reason), self.assertRaises(BenchmarkError):
                build_space(
                    MANIFEST,
                    "rules",
                    {"questions": [{"phrasings": ["q"], "calls": [bad]}]},
                )

    def test_rejects_a_question_with_neither_calls_nor_expected_behaviour(self):
        with self.assertRaises(BenchmarkError):
            build_space(MANIFEST, "rules", {"questions": [{"phrasings": ["q"]}]})


BENCHMARKS = {
    "questions": [
        {
            "phrasings": ["How late is the 99?", "99 路晚多少？"],
            "calls": [call()],
            "sample": True,
        },
        {"phrasings": ["How late is SkyTrain?"], "calls": [call(route="SkyTrain")]},
        {
            "phrasings": ["Which route is the latest?"],
            "expected": "Says it cannot rank.",
        },
    ]
}


class SpaceDefinition(unittest.TestCase):
    def setUp(self):
        self.space = build_space(
            MANIFEST, "Answer through the function.\nCite counts.\n", BENCHMARKS
        )

    def test_holds_only_the_fact_dimension_alias_tables_and_the_function(self):
        self.assertEqual(
            [t["identifier"] for t in self.space["data_sources"]["tables"]],
            [
                "workspace.transit_dev_gold.fct_trip_delay_sample",
                "workspace.transit_dev_silver.dim_routes",
                "workspace.transit_dev_silver.route_aliases",
            ],
        )
        self.assertEqual(
            [f["identifier"] for f in self.space["instructions"]["sql_functions"]],
            [FUNCTION],
        )

    def test_carries_the_instructions_text_as_one_instruction(self):
        (instruction,) = self.space["instructions"]["text_instructions"]
        self.assertEqual(
            "".join(instruction["content"]),
            "Answer through the function.\nCite counts.\n",
        )

    def test_questions_without_sql_are_not_deployed(self):
        self.assertNotIn("Which route is the latest?", answers(self.space))
        self.assertEqual(len(answers(self.space)), 3)

    def test_sample_questions_are_the_first_phrasing_of_marked_questions(self):
        self.assertEqual(
            [q["question"] for q in self.space["config"]["sample_questions"]],
            [["How late is the 99?"]],
        )

    def test_ids_are_unique_lowercase_hex_and_arrays_are_sorted_by_id(self):
        collections = [
            self.space["config"]["sample_questions"],
            self.space["instructions"]["text_instructions"],
            self.space["instructions"]["sql_functions"],
            self.space["benchmarks"]["questions"],
        ]
        ids = [item["id"] for items in collections for item in items]
        for id_ in ids:
            self.assertRegex(id_, r"^[0-9a-f]{32}$")
        self.assertEqual(len(ids), len(set(ids)))
        for items in collections:
            self.assertEqual([i["id"] for i in items], sorted(i["id"] for i in items))

    def test_is_stable_across_runs(self):
        again = build_space(
            MANIFEST, "Answer through the function.\nCite counts.\n", BENCHMARKS
        )
        self.assertEqual(again, self.space)


if __name__ == "__main__":
    unittest.main()
