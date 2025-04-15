-- Tạo bảng categories
CREATE TABLE IF NOT EXISTS categories (
    category_id INT PRIMARY KEY,
    slug TEXT,
    title TEXT
);

-- Tạo bảng brands
CREATE TABLE IF NOT EXISTS brands (
    brand_id INT PRIMARY KEY,
    brand_name TEXT,
    is_top_brand BOOLEAN
);

-- Tạo bảng authors
CREATE TABLE IF NOT EXISTS authors (
    author_id INT PRIMARY KEY,
    author_name TEXT
);

-- Tạo bảng sellers
CREATE TABLE IF NOT EXISTS sellers (
    seller_id INT PRIMARY KEY,
    seller_name TEXT,
    icon_url TEXT,
    avg_rating_point FLOAT,
    review_count INT,
    total_follower INT,
    days_since_joined INT,
    is_official BOOLEAN,
    store_level TEXT
);

-- Tạo bảng products
CREATE TABLE IF NOT EXISTS products (
    product_id INT PRIMARY KEY,
    seller_id INT REFERENCES sellers(seller_id),
    product_sku TEXT,
    product_name TEXT,
    product_url TEXT,
    images_url TEXT[],
    "description" TEXT,
    price INT,
    discount_rate INT,
    inventory_status TEXT,
    inventory_type TEXT,
    quantity_sold INT,
    rating_average FLOAT,
    review_count INT,
    specifications TEXT,
    breadcrumbs TEXT,
    category_id INT REFERENCES categories(category_id),
    is_authentic BOOLEAN,
    is_freeship_xtra BOOLEAN,
    is_top_deal BOOLEAN,
    return_reason TEXT,
    brand_id INT REFERENCES brands(brand_id),
    authors TEXT[],
    warranty_type TEXT,
    warranty_location TEXT,
    warranty_period_days INT,
    return_policy TEXT
);

-- Tạo bảng reviews
CREATE TABLE IF NOT EXISTS reviews (
    review_id INT PRIMARY KEY,
    title TEXT,
    content TEXT,
    "status" TEXT,
    rating INT,
    product_id INT REFERENCES products(product_id),
    seller_id INT REFERENCES sellers(seller_id)
);

-- Tạo bảng customers
CREATE TABLE IF NOT EXISTS customers (
    customer_id INT PRIMARY KEY,
    customer_name TEXT,
    avatar_url TEXT,
    total_review INT,
    total_thank INT,
    joined_day INT
);

-- Tạo bảng customer_review
CREATE TABLE IF NOT EXISTS customer_review (
    customer_id INT REFERENCES customers(customer_id),
    review_id INT REFERENCES reviews(review_id),
    created_ts TIMESTAMP,
    PRIMARY KEY (customer_id, review_id)
);

-- Tạo bảng customer_purchase
CREATE TABLE IF NOT EXISTS customer_purchase (
    customer_id INT REFERENCES customers(customer_id),
    product_id INT REFERENCES products(product_id),
    seller_id INT REFERENCES sellers(seller_id),
    purchased_ts TIMESTAMP,
    PRIMARY KEY (customer_id, product_id, seller_id)
);
