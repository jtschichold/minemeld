#  Copyright 2015 Palo Alto Networks, Inc
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import jmespath
import logging
import antlr4
import operator
import copy
from typing import (
    Optional, Tuple
)

from .BoolExprParser import BoolExprParser  # noqa
from .BoolExprLexer import BoolExprLexer  # noqa
from .BoolExprListener import BoolExprListener  # noqa


LOG = logging.getLogger(__name__)


class _BECompiler(BoolExprListener):
    def exitExpression(self, ctx):
        self.expression = jmespath.compile(ctx.getText())

    def exitComparator(self, ctx):
        comparator = ctx.getText()
        if comparator == '==':
            self.comparator = operator.eq
        elif comparator == '<':
            self.comparator = operator.lt
        elif comparator == '<=':
            self.comparator = operator.le
        elif comparator == '>':
            self.comparator = operator.gt
        elif comparator == '>=':
            self.comparator = operator.ge
        elif comparator == '!=':
            self.comparator = operator.ne

    def exitValue(self, ctx):
        if ctx.STRING() is not None:
            self.value = ctx.STRING().getText()[1:-1]
        elif ctx.NUMBER() is not None:
            self.value = int(ctx.NUMBER().getText())
        elif ctx.getText() == 'null':
            self.value = None
        elif ctx.getText() == 'false':
            self.value = False
        elif ctx.getText() == 'true':
            self.value = True


class Condition:
    def __init__(self, s):
        self.expression, self.comparator, self.value = self._parse_boolexpr(s)

    def _parse_boolexpr(self, s):
        lexer = BoolExprLexer(
            antlr4.InputStream(s)
        )
        stream = antlr4.CommonTokenStream(lexer)
        parser = BoolExprParser(stream)
        tree = parser.booleanExpression()

        eb = _BECompiler()
        walker = antlr4.ParseTreeWalker()
        walker.walk(eb, tree)

        return eb.expression, eb.comparator, eb.value

    def eval(self, i):
        try:
            r = self.expression.search(i)
        except jmespath.exceptions.JMESPathError:
            LOG.debug("Exception in eval: ", exc_info=True)
            r = None

        # XXX this is a workaround for a bug in JMESPath
        if r == 'null':
            r = None

        # in pyhon3 only operators eq ne do not throw an error when handling None
        if r is None or self.value is None:
            if self.comparator not in [operator.eq, operator.ne]:
                return False

        return self.comparator(r, self.value)


class Filters:
    """Implements a set of filters to be applied to indicators.
    Used by mineneld.ft.base.BaseFT for ingress and egress filters.

    Args:
        filters (list): list of filters.
    """
    def __init__(self, filters):
        self.filters = []

        for f in filters:
            cf = {
                'name': f.get('name', 'filter_%d' % len(self.filters)),
                'conditions': [],
                'actions': []
            }

            fconditions = f.get('conditions', None)
            if fconditions is None:
                fconditions = []
            for c in fconditions:
                cf['conditions'].append(Condition(c))

            for a in f.get('actions'):
                cf['actions'].append(a)

            self.filters.append(cf)

    def apply(self, origin: Optional[str]=None, method: Optional[str]=None,
            indicator: Optional[str]=None, value: Optional[dict]=None) -> Tuple[Optional[str], Optional[dict]]:
        if value is None:
            d = {}
        else:
            d = copy.copy(value)

        if indicator is not None:
            d['__indicator'] = indicator

        if method is not None:
            d['__method'] = method

        if origin is not None:
            d['__origin'] = origin

        for f in self.filters:
            LOG.debug("evaluating filter %s", f['name'])

            r = True
            for c in f['conditions']:
                r &= c.eval(d)

            if not r:
                continue

            for a in f['actions']:
                if a == 'accept':
                    if value is None:
                        return indicator, None

                    d.pop('__indicator')
                    d.pop('__origin', None)
                    d.pop('__method', None)

                    return indicator, d

                elif a == 'drop':
                    return None, None

        LOG.debug("no matching filter, default accept")

        if value is None:
            return indicator, None

        d.pop('__indicator')
        d.pop('__origin', None)
        d.pop('__method', None)

        return indicator, d
