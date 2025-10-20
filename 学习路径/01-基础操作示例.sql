-- =============================================
-- SQL基础操作示例代码
-- 适合初学者学习和参考
-- =============================================

-- 1. 数据库和表的基本操作
-- =============================================

-- 创建数据库（SQLite会自动创建）
-- 在SQLite中，数据库就是一个文件

-- 创建学生表
CREATE TABLE students (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    age INTEGER,
    grade TEXT,
    gender TEXT,
    email TEXT
);

-- 创建成绩表
CREATE TABLE scores (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    student_id INTEGER,
    subject TEXT,
    score REAL,
    exam_date DATE,
    FOREIGN KEY (student_id) REFERENCES students(id)
);

-- 2. 插入数据示例
-- =============================================

-- 插入学生数据
INSERT INTO students (name, age, grade, gender, email) VALUES 
('小明', 15, '九年级', '男', 'xiaoming@school.com'),
('小红', 14, '八年级', '女', 'xiaohong@school.com'),
('小刚', 16, '高一', '男', 'xiaogang@school.com'),
('小丽', 15, '九年级', '女', 'xiaoli@school.com'),
('小强', 14, '八年级', '男', 'xiaoqiang@school.com'),
('小美', 16, '高一', '女', 'xiaomei@school.com');

-- 插入成绩数据
INSERT INTO scores (student_id, subject, score, exam_date) VALUES 
(1, '数学', 85, '2024-01-15'),
(1, '语文', 92, '2024-01-15'),
(1, '英语', 78, '2024-01-15'),
(2, '数学', 90, '2024-01-15'),
(2, '语文', 88, '2024-01-15'),
(2, '英语', 95, '2024-01-15'),
(3, '数学', 76, '2024-01-15'),
(3, '语文', 85, '2024-01-15'),
(3, '英语', 82, '2024-01-15'),
(4, '数学', 92, '2024-01-15'),
(4, '语文', 89, '2024-01-15'),
(4, '英语', 91, '2024-01-15'),
(5, '数学', 68, '2024-01-15'),
(5, '语文', 75, '2024-01-15'),
(5, '英语', 72, '2024-01-15'),
(6, '数学', 88, '2024-01-15'),
(6, '语文', 94, '2024-01-15'),
(6, '英语', 89, '2024-01-15');

-- 3. 基础查询示例
-- =============================================

-- 查询所有学生信息
SELECT * FROM students;

-- 查询特定字段
SELECT name, age, grade FROM students;

-- 查询学生总数
SELECT COUNT(*) as total_students FROM students;

-- 4. 条件查询示例
-- =============================================

-- 查询年龄大于15岁的学生
SELECT * FROM students WHERE age > 15;

-- 查询九年级的学生
SELECT * FROM students WHERE grade = '九年级';

-- 查询年龄在14到16岁之间的学生
SELECT * FROM students WHERE age BETWEEN 14 AND 16;

-- 查询姓"小"的学生
SELECT * FROM students WHERE name LIKE '小%';

-- 查询名字中包含"明"的学生
SELECT * FROM students WHERE name LIKE '%明%';

-- 5. 排序示例
-- =============================================

-- 按年龄升序排列
SELECT * FROM students ORDER BY age;

-- 按年龄降序排列
SELECT * FROM students ORDER BY age DESC;

-- 先按年级排序，再按年龄排序
SELECT * FROM students ORDER BY grade, age;

-- 6. 限制结果示例
-- =============================================

-- 查询前3名学生
SELECT * FROM students LIMIT 3;

-- 查询年龄最大的3名学生
SELECT * FROM students ORDER BY age DESC LIMIT 3;

-- 跳过前2名，查询接下来的3名学生
SELECT * FROM students LIMIT 2, 3;

-- 7. 聚合函数示例
-- =============================================

-- 计算平均年龄
SELECT AVG(age) as average_age FROM students;

-- 找出最大年龄和最小年龄
SELECT MAX(age) as max_age, MIN(age) as min_age FROM students;

-- 计算年龄总和
SELECT SUM(age) as total_age FROM students;

-- 8. 分组查询示例
-- =============================================

-- 按年级统计学生数量
SELECT grade, COUNT(*) as student_count 
FROM students 
GROUP BY grade;

-- 按年级计算平均年龄
SELECT grade, AVG(age) as avg_age 
FROM students 
GROUP BY grade;

-- 按性别和年级分组统计
SELECT gender, grade, COUNT(*) as count 
FROM students 
GROUP BY gender, grade;

-- 9. 分组后筛选示例
-- =============================================

-- 找出学生数量超过1人的年级
SELECT grade, COUNT(*) as student_count 
FROM students 
GROUP BY grade 
HAVING COUNT(*) > 1;

-- 找出平均年龄超过15岁的年级
SELECT grade, AVG(age) as avg_age 
FROM students 
GROUP BY grade 
HAVING AVG(age) > 15;

-- 10. 成绩分析示例
-- =============================================

-- 查询所有成绩
SELECT * FROM scores;

-- 计算各科目平均分
SELECT subject, AVG(score) as avg_score 
FROM scores 
GROUP BY subject;

-- 找出每个科目的最高分
SELECT subject, MAX(score) as max_score 
FROM scores 
GROUP BY subject;

-- 统计各分数段的人数
SELECT 
    CASE 
        WHEN score >= 90 THEN '优秀'
        WHEN score >= 80 THEN '良好'
        WHEN score >= 60 THEN '及格'
        ELSE '不及格'
    END as grade_level,
    COUNT(*) as student_count
FROM scores 
GROUP BY grade_level;

-- 11. 综合查询示例
-- =============================================

-- 查询每个学生的平均成绩
SELECT 
    s.name,
    s.grade,
    AVG(sc.score) as avg_score
FROM students s
JOIN scores sc ON s.id = sc.student_id
GROUP BY s.id, s.name, s.grade;

-- 查询平均成绩大于80分的学生
SELECT 
    s.name,
    s.grade,
    AVG(sc.score) as avg_score
FROM students s
JOIN scores sc ON s.id = sc.student_id
GROUP BY s.id, s.name, s.grade
HAVING AVG(sc.score) > 80;

-- 按平均成绩降序排列，显示前3名
SELECT 
    s.name,
    s.grade,
    AVG(sc.score) as avg_score
FROM students s
JOIN scores sc ON s.id = sc.student_id
GROUP BY s.id, s.name, s.grade
ORDER BY avg_score DESC
LIMIT 3;

-- 12. 实用查询技巧
-- =============================================

-- 查询每个年级的男女比例
SELECT 
    grade,
    gender,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY grade), 2) as percentage
FROM students 
GROUP BY grade, gender;

-- 查询各科目的成绩分布
SELECT 
    subject,
    COUNT(*) as total_students,
    SUM(CASE WHEN score >= 90 THEN 1 ELSE 0 END) as excellent_count,
    SUM(CASE WHEN score >= 80 AND score < 90 THEN 1 ELSE 0 END) as good_count,
    SUM(CASE WHEN score >= 60 AND score < 80 THEN 1 ELSE 0 END) as pass_count,
    SUM(CASE WHEN score < 60 THEN 1 ELSE 0 END) as fail_count
FROM scores 
GROUP BY subject;

-- 13. 数据更新示例
-- =============================================

-- 更新学生年龄
UPDATE students SET age = 16 WHERE name = '小明';

-- 更新成绩
UPDATE scores SET score = 95 WHERE student_id = 1 AND subject = '数学';

-- 14. 数据删除示例
-- =============================================

-- 删除特定学生的成绩记录
DELETE FROM scores WHERE student_id = 6;

-- 删除特定学生（注意外键约束）
DELETE FROM students WHERE name = '小美';

-- 15. 表结构修改示例
-- =============================================

-- 添加新字段
ALTER TABLE students ADD COLUMN phone TEXT;

-- 更新新字段的值
UPDATE students SET phone = '13800138000' WHERE id = 1;

-- 16. 索引创建示例
-- =============================================

-- 为常用查询字段创建索引
CREATE INDEX idx_students_grade ON students(grade);
CREATE INDEX idx_students_age ON students(age);
CREATE INDEX idx_scores_subject ON scores(subject);
CREATE INDEX idx_scores_student_id ON scores(student_id);

-- 17. 视图创建示例
-- =============================================

-- 创建学生成绩视图
CREATE VIEW student_scores AS
SELECT 
    s.id,
    s.name,
    s.grade,
    sc.subject,
    sc.score,
    sc.exam_date
FROM students s
JOIN scores sc ON s.id = sc.student_id;

-- 使用视图查询
SELECT * FROM student_scores WHERE grade = '九年级';

-- 18. 常用统计查询
-- =============================================

-- 统计各年级男女学生数量
SELECT 
    grade,
    gender,
    COUNT(*) as count
FROM students 
GROUP BY grade, gender
ORDER BY grade, gender;

-- 统计各科目成绩分布
SELECT 
    subject,
    COUNT(*) as total_count,
    AVG(score) as avg_score,
    MAX(score) as max_score,
    MIN(score) as min_score
FROM scores 
GROUP BY subject;

-- 19. 数据验证查询
-- =============================================

-- 检查是否有重复的学生姓名
SELECT name, COUNT(*) as count
FROM students 
GROUP BY name 
HAVING COUNT(*) > 1;

-- 检查是否有无效的成绩（小于0或大于100）
SELECT * FROM scores WHERE score < 0 OR score > 100;

-- 检查是否有学生没有成绩记录
SELECT s.* 
FROM students s
LEFT JOIN scores sc ON s.id = sc.student_id
WHERE sc.id IS NULL;

-- 20. 性能优化示例
-- =============================================

-- 使用EXPLAIN查看查询计划
EXPLAIN QUERY PLAN SELECT * FROM students WHERE grade = '九年级';

-- 使用LIMIT限制结果数量
SELECT * FROM students ORDER BY age DESC LIMIT 10;

-- 只查询需要的字段
SELECT name, grade FROM students WHERE age > 15;

-- =============================================
-- 总结：这些示例涵盖了SQL基础操作的主要内容
-- 建议按顺序学习，逐步掌握每个概念
-- ============================================= 