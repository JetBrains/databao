import pytest

from databao.agents.frontend.text_frontend import escape_markdown_text


@pytest.mark.parametrize(
    "input_text, expected_output",
    [
        ("Price is $100.", "Price is \\$100."),
        ("Amount: $42.75, thank you.", "Amount: \\$42.75, thank you."),
        ("$5 threshold", "\\$5 threshold"),
        ("Just a regular string.", "Just a regular string."),
        ("", ""),
        ("Math: $x^2 + y^2 = z^2$", "Math: $x^2 + y^2 = z^2$"),
        ("Variable $a$ and $b$", "Variable $a$ and $b$"),
        ("A single $m$ variable.", "A single $m$ variable."),
        ("Display math: $$E=mc^2$$", "Display math: $$E=mc^2$$"),
        ("Equation $$ a_n = a_{n-1} + d $$", "Equation $$ a_n = a_{n-1} + d $$"),
        ("$1$", "\\$1$"),
        ("$1.5$", "\\$1.5$"),
        ("$1.5 and $0.5", "\\$1.5 and \\$0.5"),
        ("a number $5$ wow", "a number \\$5$ wow"),
        ("Escaped \\$var in text.", "Escaped \\$var in text."),
        ("Identifier $P1.", "Identifier $P1."),
        ("Not math: $A1=Value", "Not math: $A1=Value"),
        ("math: $A1=Value$", "math: $A1=Value$"),
        # Dollar sign AFTER number
        ("Amount is 100$.", "Amount is 100$."),
        ("It costs 25.50$.", "It costs 25.50$."),
        ("This $ should not change, nor should 1\\$.", "This $ should not change, nor should 1\\$."),
        # ("$$1$$", "$$1$$"),
        # ("This is an escaped \\$50 amount.", "This is an escaped \\$50 amount."),
        # ("5$ and 10$", "5$ and 10$"),
        # ("Code $Rate25 and $Foo.", "Code $Rate25 and $Foo."),
        # ("Math $x$ and price $P1: $75. Then 120$.", "Math $x$ and price \\$P1: \\$75. Then 120\\$."),
        # ("This $ should not change, nor should 1$.", "This $ should not change, nor should 1$."),
        # (
        #     "Costs $10, $25.50, and $0.99. Also 5$ and 10$. And $ID1, $ID2.",
        #     "Costs \\$10, \\$25.50, and \\$0.99. Also 5\\$ and 10\\$. And \\$ID1, \\$ID2.",
        # ),
        # ("$05.00 dollars ($) and $var$", "\\$05.00 dollars ($) and $var$"),
        ("$\\$ $100", "$\\$ \\$100"),
        ("$05.00", "\\$05.00"),
        ("05.00$", "05.00$"),
        ("$ID007", "$ID007"),
        ("foo ~50", "foo \\~50"),
        ("foo ~50~", "foo \\~50~"),
        ("foo ~foo~", "foo ~foo~"),
        ("revenue (~420) vs 2023 (~360)", "revenue (\\~420) vs 2023 (\\~360)"),
        ("revenue (~$420) vs 2023 (~$360)", "revenue (\\~\\$420) vs 2023 (\\~\\$360)"),
    ],
)
def test_escape_text(input_text: str, expected_output: str) -> None:
    assert escape_markdown_text(input_text) == expected_output
