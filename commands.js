const consoleDiv = document.getElementById('console');
const editor = document.getElementById('editor');
const runBtn = document.getElementById('run');

const variables = {};
let inBlockComment = false;

function clearConsole() {
  consoleDiv.innerHTML = '';
}

function writeOutput(message, type = 'log') {
  const div = document.createElement('div');
  div.textContent = message;
  div.className = type;
  consoleDiv.appendChild(div);
  consoleDiv.scrollTop = consoleDiv.scrollHeight;
}

function showHelp() {
  writeOutput('logix help');
  writeOutput('-----------');
  writeOutput('set "name" to \'value\'');
  writeOutput('log "name"');
  writeOutput('upper <text>');
  writeOutput('reverse <text>');
  writeOutput('math <expression>');
  writeOutput('if <cond> then <command>');
  writeOutput('while <cond> do <command>');
  writeOutput('-- single line comment');
  writeOutput('/-- multi line comment --/');
}

function resolveVars(text) {
  return text.replace(/"([^"]+)"/g, (_, name) => {
    return variables[name] ?? `"${name}"`;
  });
}

function evalCondition(cond) {
  try {
    return Function(`return (${resolveVars(cond)})`)();
  } catch {
    writeOutput('invalid condition', 'error');
    return false;
  }
}

function runCommand(raw) {
  let command = raw.trim();
  if (!command) return;

  if (command.startsWith('/--')) {
    inBlockComment = true;
    return;
  }

  if (command.endsWith('--/')) {
    inBlockComment = false;
    return;
  }

  if (inBlockComment) return;
  if (command.startsWith('--')) return;

  if (command === '/help') {
    showHelp();
    return;
  }

  if (command.startsWith('if ')) {
    const m = command.match(/^if (.+) then (.+)$/);
    if (m && evalCondition(m[1])) runCommand(m[2]);
    return;
  }

  if (command.startsWith('while ')) {
    const m = command.match(/^while (.+) do (.+)$/);
    if (m) {
      let safety = 1000;
      while (evalCondition(m[1]) && safety-- > 0) {
        runCommand(m[2]);
      }
    }
    return;
  }

  const setMatch = command.match(/^set "([^"]+)" to '([^']*)'$/);
  if (setMatch) {
    let value = setMatch[2];
    value = isNaN(value) ? value : Number(value);
    variables[setMatch[1]] = value;
    writeOutput(`set ${setMatch[1]} = ${value}`, 'variable');
    return;
  }

  const [cmd, ...rest] = command.split(' ');
  const args = rest.join(' ');

  switch (cmd) {
    case 'log':
      writeOutput(resolveVars(args));
      break;
    case 'upper':
      writeOutput(resolveVars(args).toUpperCase());
      break;
    case 'reverse':
      writeOutput(resolveVars(args).split('').reverse().join(''));
      break;
    case 'math':
      try {
        writeOutput(eval(resolveVars(args)));
      } catch {
        writeOutput('invalid math', 'error');
      }
      break;
    default:
      writeOutput(`unknown command: ${cmd}`, 'error');
  }
}

runBtn.addEventListener('click', () => {
  clearConsole();
  Object.keys(variables).forEach(k => delete variables[k]);
  inBlockComment = false;

  editor.value.split('\n').forEach(line => runCommand(line));
});
