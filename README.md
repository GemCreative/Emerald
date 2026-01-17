
<img src="emeraldicon.png" width="50" alt="logo"/> Emerald
===============
**© Copyright 2026 Emerald**

---

[![badgey badge](https://img.shields.io/badge/Emerald_Basic-Latest-green)](https://github.com/AModernAnimator/Emerald/releases/tag/Basic)


> Emerald is a scripting language created by **@AModernAnimator** and **@Sushi-byte-glitch**, designed for **web development**, **data management**, and **applications**.  
> It comes in **3 versions**: **Basic**, **Dull**, and **Shine**, each increasing in complexity, power, and efficiency. *Current versions : Basic v.1.0 and Dull v.0.5 (in progress)*

---

Emerald is inspired by **Python’s readability**, with some influences from **JavaScript**, and derives from **Python**.

---

## 🚀 Versions

| Version | Description | Best For |
|---------|-------------|----------|
| **Basic** | Simple, beginner-friendly scripting | Learning, simple automation |
| **Dull** | More features, more control | Intermediate scripts |
| **Shine** | Full-featured and optimized | Advanced development |

---

## 🧪 Example Script (Basic)

```emerald
<type=basic>
-- this is a comment
// this is a
multi line comment //

while(true)[
  delay=5000
  output.log="Hello world!"
]
```

# 📥 Install Emerald

**Install Emerald in your repo:**

 1. Clone the repository

  ```git clone https://github.com/AModernAnimator/Emerald.git```


 2. Navigate to the Basic folder

  ```cd Emerald/Basic```


 3. Run the interpreter

   ```python emerald.py <script.emlg>```

# 🔧 Quick Start
## Basic
**✅ Create a script**

Create a file called ```example.emlg```:
```
var x = 10
while({x} > 0)[
  output.log="Countdown: {x}"
  delay=1000
  var x = {x} - 1
]
```
✅ Run it
```python emerald.py example.emlg```

📌 Notes

- delay is measured in milliseconds

- Variables use curly braces for interpolation:
  ```ini
  output.log="Hello {name}"
  ```


Comments can be:
```arduino
-- single line
// multi-line //
```

