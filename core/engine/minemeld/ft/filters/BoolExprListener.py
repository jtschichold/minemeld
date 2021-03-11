# Generated from BoolExpr.g4 by ANTLR 4.8
from antlr4 import *
if __name__ is not None and "." in __name__:
    from .BoolExprParser import BoolExprParser
else:
    from BoolExprParser import BoolExprParser

# flake8: noqa


# This class defines a complete listener for a parse tree produced by BoolExprParser.
class BoolExprListener(ParseTreeListener):

    # Enter a parse tree produced by BoolExprParser#booleanExpression.
    def enterBooleanExpression(self, ctx:BoolExprParser.BooleanExpressionContext):
        pass

    # Exit a parse tree produced by BoolExprParser#booleanExpression.
    def exitBooleanExpression(self, ctx:BoolExprParser.BooleanExpressionContext):
        pass


    # Enter a parse tree produced by BoolExprParser#expression.
    def enterExpression(self, ctx:BoolExprParser.ExpressionContext):
        pass

    # Exit a parse tree produced by BoolExprParser#expression.
    def exitExpression(self, ctx:BoolExprParser.ExpressionContext):
        pass


    # Enter a parse tree produced by BoolExprParser#functionExpression.
    def enterFunctionExpression(self, ctx:BoolExprParser.FunctionExpressionContext):
        pass

    # Exit a parse tree produced by BoolExprParser#functionExpression.
    def exitFunctionExpression(self, ctx:BoolExprParser.FunctionExpressionContext):
        pass


    # Enter a parse tree produced by BoolExprParser#noArgs.
    def enterNoArgs(self, ctx:BoolExprParser.NoArgsContext):
        pass

    # Exit a parse tree produced by BoolExprParser#noArgs.
    def exitNoArgs(self, ctx:BoolExprParser.NoArgsContext):
        pass


    # Enter a parse tree produced by BoolExprParser#oneOrMoreArgs.
    def enterOneOrMoreArgs(self, ctx:BoolExprParser.OneOrMoreArgsContext):
        pass

    # Exit a parse tree produced by BoolExprParser#oneOrMoreArgs.
    def exitOneOrMoreArgs(self, ctx:BoolExprParser.OneOrMoreArgsContext):
        pass


    # Enter a parse tree produced by BoolExprParser#comparator.
    def enterComparator(self, ctx:BoolExprParser.ComparatorContext):
        pass

    # Exit a parse tree produced by BoolExprParser#comparator.
    def exitComparator(self, ctx:BoolExprParser.ComparatorContext):
        pass


    # Enter a parse tree produced by BoolExprParser#value.
    def enterValue(self, ctx:BoolExprParser.ValueContext):
        pass

    # Exit a parse tree produced by BoolExprParser#value.
    def exitValue(self, ctx:BoolExprParser.ValueContext):
        pass



del BoolExprParser