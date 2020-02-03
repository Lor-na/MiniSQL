# !_*_ coding:utf-8 _*_

import API
from prompt_toolkit import PromptSession
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.lexers import PygmentsLexer
from pygments.lexers.sql import SqlLexer


sql_completer = WordCompleter(['select', 'insert', 'update', 'from', 'where'], ignore_case=True)


def main():
    API.readTables()
    session = PromptSession(lexer=PygmentsLexer(SqlLexer))
    end = True
    res = ''
    while True:
        try:
            if end:
                text = session.prompt('minisql>', completer=sql_completer)
            else:
                text = session.prompt('      ->', completer=sql_completer)
        except KeyboardInterrupt:
            end = True
            res = ''
            continue
        except EOFError:
            break
        else:
            text = text.strip()
            res += text
            if res[-1] == ';':
                end = True
                # handle res
                try:
                    API.interpret(res)
                    res = ''
                except API.SQLException:
                    res = ''
                    continue
                except EOFError:
                    break
            else:
                end = False
    API.saveTables()
    API.bufferList.saveAllBuffer()
    print('GoodBye!')


if __name__ == "__main__":
    main()
