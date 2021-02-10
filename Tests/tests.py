from subprocess import Popen, PIPE, run


def run_scripts(commands):
    p = Popen(["cmake-build-debug/SQLCloneExp", "test.db"], stdin=PIPE, stdout=PIPE)

    for command in commands:
        p.stdin.write(command)
        p.stdin.flush()
    p.stdin.close()

    res = p.stdout.read().decode("utf-8")
    p.terminate()
    run(["chmod", "+rw", "test.db"])
    return res.split('\n')


def simple_tests():
    run(["rm", "-rf", "test.db"])

    tests = {
        (b'insert 1 user1 person1@example.com\n', b'.exit\n'): ['db > Executed.', 'db > '],
        (b'select\n', b'.exit\n'): ['db > (1, user1, person1@example.com)', 'Executed.', 'db > '],
    }

    for test, out in tests.items():
        assert out == run_scripts(test)


def load_test():
    run(["rm", "-rf", "test.db"])
    # load testing

    commands = []
    for i in range(1401):
        command = "insert {} user{}, person{}@example.com\n".format(i, i, i)
        commands.append(bytes(command, 'utf8'))
    commands.append(b'.exit\n')

    out = run_scripts(commands=commands)
    assert out[-2] == 'db > Error: Table full.'


def field_test():
    run(["rm", "-rf", "test.db"])
    # testing field length
    long_insert_command = "insert 1 " + "a" * 32 + " " + "a" * 255 + "\n"

    commands = [bytes(long_insert_command, 'utf8'), b'select\n', b'.exit\n']
    expected_out = ['db > Executed.', "db > (1, " + "a" * 32 + ", " + "a" * 255 + ")", 'Executed.', 'db > ']

    assert expected_out == run_scripts(commands)


if __name__ == '__main__':

    simple_tests()
    load_test()
    field_test()